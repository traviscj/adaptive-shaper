#!/usr/local/bin/python3
"""adaptive-shaper: cake-autorate-style controller for the OPNsense download pipe.

Modulates dummynet pipe DOWN_PIPE bandwidth at runtime (dnctl) so the fq_codel
queue stays the bottleneck even as Comcast's delivered DOCSIS rate swings.
Cuts on latency-under-load, grows when clean, reverts toward BASE when idle.
Runtime dnctl bw reconfig preserves the fq_codel sched (verified 2026-08-13).

v2 (2026-10-28, after Waveform C/C at midnight):
  - RTT probes are async (Popen, harvested each tick, killed at PROBE_TIMEOUT_S)
    so a lost/slow ping can never stall the tick. Tick is 0.5 s.
  - Severity-scaled cuts: delta > RTT_SEVERE_MS (or 2 consecutive probe
    timeouts) jumps straight to DELIVERED_FRAC x recent delivered WAN rx,
    i.e. below what the CMTS is actually handing us.
  - "loaded" also triggers on absolute rx (LOAD_ABS_MBIT), so a line delivering
    far below the pipe rate still counts as loaded.
  - Idle revert goes to the last loaded rate (+10%) and creeps to BASE over
    ~10 min instead of jumping straight back to BASE.
  - Floor 950.  Growth only when rx is actually pressing on the pipe.
  - Telemetry: tick dt, work time, rx inst/peak/ewma, probes in flight, losses,
    event column; TICK_GAP log lines.
"""

import json
import os
import re
import signal
import subprocess
import time
from datetime import datetime

# ---- tunables -------------------------------------------------------------
DOWN_PIPE = 2                 # WANDown
IFACE = "igc1"                # WAN
FLOOR_MBIT = 950
CEIL_MBIT = 2100
BASE_MBIT = 1800              # idle reversion target / startup rate
RTT_LO_MS = 15.0              # below this while loaded -> grow
RTT_HI_MS = 40.0              # above this while loaded -> moderate cut
RTT_SEVERE_MS = 100.0         # above this while loaded -> severe cut
CUT_FACTOR = 0.85             # moderate cut
SEVERE_MIN_FACTOR = 0.60      # severe cut never goes below rate * this ...
DELIVERED_FRAC = 0.88         # ... and targets this * recent delivered rx
RX_PEAK_WINDOW_S = 2.0
GROW_MBIT = 10                # per clean loaded tick (= 20 Mbit/s at 0.5 s)
GROW_HOLDOFF_S = 4.0          # no growth this soon after a cut
GROW_LOAD_FRAC = 0.80         # only grow when rx > this * rate (pipe binding)
LOAD_FRAC = 0.60              # rx > this * rate  -> "loaded"
LOAD_ABS_MBIT = 700           # ... or rx > this absolute
IDLE_FRAC = 0.20              # rx < this * rate  -> "idle" (baseline updates)
CUT_REFRACTORY_S = 2.0
IDLE_REVERT_AFTER_S = 60.0    # idle this long before stepping toward target
IDLE_REVERT_STEP = 25         # Mbit per tick while reverting
IDLE_REVERT_HEADROOM = 1.10   # revert first to last loaded rate * this
IDLE_CREEP_MBIT_S = 1.0       # then target creeps toward BASE at this rate
DRIFT_CHECK_S = 30.0
REFLECTORS = ["1.1.1.1", "1.0.0.1", "8.8.8.8", "9.9.9.9"]
PROBE_WAIT_MS = 900           # ping -W
PROBE_TIMEOUT_S = 1.2         # kill + count as loss after this
TICK_S = 0.5
TICK_GAP_WARN_S = 1.5
HEARTBEAT_S = 300.0

LOG = "/var/log/adaptive_shaper.log"
LOG_MAX_BYTES = 5 * 1024 * 1024
STATUS = "/var/run/adaptive_shaper.status"
TELEM = "/var/log/adaptive_shaper_telemetry.csv"
TELEM_MAX_BYTES = 25 * 1024 * 1024   # rotate to .0, keep one
TELEM_IDLE_EVERY = 20                # decimate idle ticks to 1-in-20 (10 s)
TELEM_HEADER = ("ts,dt_ms,work_ms,rate_mbit,rx_mbps,rx_ewma,rx_peak,rtt_ms,"
                "reflector,delta_ms,loaded,inflight,loss_streak,event\n")
# ---------------------------------------------------------------------------

running = True


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def log(msg):
    try:
        if os.path.exists(LOG) and os.path.getsize(LOG) > LOG_MAX_BYTES:
            os.rename(LOG, LOG + ".0")
        with open(LOG, "a") as f:
            f.write("%s %s\n" % (now_iso(), msg))
    except OSError:
        pass


def run(cmd, timeout=3):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


_telem_f = None


def telem(row):
    """Per-tick CSV row (list of already-formatted fields)."""
    global _telem_f
    try:
        if _telem_f is None:
            if os.path.exists(TELEM):
                with open(TELEM) as f:
                    hdr = f.readline()
                if hdr != TELEM_HEADER:      # schema changed: park old file
                    os.rename(TELEM, TELEM + ".v1")
            fresh = not os.path.exists(TELEM)
            _telem_f = open(TELEM, "a", buffering=1)
            if fresh:
                _telem_f.write(TELEM_HEADER)
        _telem_f.write(",".join(row) + "\n")
        if _telem_f.tell() > TELEM_MAX_BYTES:
            _telem_f.close()
            _telem_f = None
            os.rename(TELEM, TELEM + ".0")
    except OSError:
        _telem_f = None


def fmt(x, spec="%.1f"):
    return spec % x if x is not None else ""


def pipe_bw_mbit():
    """Current bw of DOWN_PIPE per dnctl, in Mbit; None if pipe absent."""
    try:
        out = run(["/sbin/dnctl", "pipe", "show"]).stdout
    except Exception:
        return None
    m = re.search(r"^0*%d:\s+([\d.]+)\s+([GM])bit/s" % DOWN_PIPE, out, re.M)
    if not m:
        return None
    val = float(m.group(1))
    return val * 1000.0 if m.group(2) == "G" else val


def sched_is_fq_codel():
    try:
        out = run(["/sbin/dnctl", "sched", "show"]).stdout
    except Exception:
        return False
    return bool(re.search(r"sched %d type FQ_CODEL" % DOWN_PIPE, out))


def set_bw(mbit):
    mbit = int(round(mbit))
    try:
        r = run(["/sbin/dnctl", "pipe", str(DOWN_PIPE), "config",
                 "bw", "%dMbit/s" % mbit])
    except subprocess.TimeoutExpired:
        log("ERROR dnctl set %d: timeout" % mbit)
        return False
    if r.returncode != 0:
        log("ERROR dnctl set %d: %s" % (mbit, r.stderr.strip()))
        return False
    return True


def wan_ibytes():
    """Ibytes from the Link# row for IFACE; None on parse failure."""
    try:
        out = run(["/usr/bin/netstat", "-ibn", "-I", IFACE]).stdout
    except Exception:
        return None
    for line in out.splitlines():
        if "<Link#" in line:
            parts = line.split()
            try:
                return int(parts[7])
            except (IndexError, ValueError):
                return None
    return None


class Prober:
    """Fire-and-forget pings; harvest() never blocks."""

    def __init__(self):
        self.inflight = []        # (popen, host, start_monotonic)

    def fire(self, host):
        try:
            p = subprocess.Popen(
                ["/sbin/ping", "-n", "-c", "1", "-W", str(PROBE_WAIT_MS), host],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        except OSError as e:
            log("ERROR ping spawn: %s" % e)
            return
        self.inflight.append((p, host, time.monotonic()))

    def harvest(self, now):
        """-> list of (host, rtt_ms or None). None = loss/timeout."""
        done, keep = [], []
        for p, host, t in self.inflight:
            if p.poll() is not None:
                out = p.stdout.read() if p.stdout else ""
                p.stdout.close()
                m = re.search(r"time=([\d.]+)\s*ms", out)
                done.append((host, float(m.group(1)) if m else None))
            elif now - t > PROBE_TIMEOUT_S:
                p.kill()
                p.wait()
                p.stdout.close()
                done.append((host, None))
            else:
                keep.append((p, host, t))
        self.inflight = keep
        return done

    def shutdown(self):
        for p, _, _ in self.inflight:
            p.kill()
            p.wait()


def on_term(signum, frame):
    global running
    running = False


def main():
    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)

    log("startup: waiting for pipe %d with FQ_CODEL sched" % DOWN_PIPE)
    while running and not (pipe_bw_mbit() is not None and sched_is_fq_codel()):
        time.sleep(5)
    if not running:
        return

    rate = float(BASE_MBIT)
    set_bw(rate)
    log("startup v2: took control, rate=%d (floor=%d ceil=%d base=%d tick=%.1f "
        "hi/severe=%.0f/%.0f delivered_frac=%.2f)"
        % (rate, FLOOR_MBIT, CEIL_MBIT, BASE_MBIT, TICK_S,
           RTT_HI_MS, RTT_SEVERE_MS, DELIVERED_FRAC))

    prober = Prober()
    baselines = {}            # reflector -> EWMA baseline ms
    refl_i = 0
    prev_bytes = wan_ibytes()
    prev_t = time.monotonic()
    rx_ewma = 0.0
    rx_hist = []              # (t, rx_inst) for peak window
    last_cut_t = 0.0
    loaded_last_t = time.monotonic()
    last_loaded_rate = rate
    last_drift_t = time.monotonic()
    last_heartbeat_t = 0.0
    last_logged_rate = rate
    loss_streak = 0
    tick = 0
    stats = {"cuts": 0, "severe_cuts": 0, "min_rate": rate, "max_dt_ms": 0.0,
             "tick_gaps": 0, "probes": 0, "losses": 0}
    last_rtt, last_host, last_delta = None, "", None

    while running:
        t0 = time.monotonic()
        tick += 1
        dt = t0 - prev_t
        event = ""

        # -- rx rate from Ibytes delta
        rx = None
        b = wan_ibytes()
        if b is not None and prev_bytes is not None and b >= prev_bytes and dt > 0:
            rx = (b - prev_bytes) * 8.0 / dt / 1e6
            rx_ewma = 0.5 * rx_ewma + 0.5 * rx
            rx_hist.append((t0, rx))
        prev_bytes = b if b is not None else prev_bytes
        prev_t = t0
        rx_hist = [(t, v) for t, v in rx_hist if t0 - t <= RX_PEAK_WINDOW_S]
        rx_peak = max([v for _, v in rx_hist] or [0.0])

        if tick > 1 and dt > TICK_GAP_WARN_S:
            stats["tick_gaps"] += 1
            log("TICK_GAP dt=%.2fs" % dt)
            event = "gap"
        if tick > 1:
            stats["max_dt_ms"] = max(stats["max_dt_ms"], dt * 1000.0)

        loaded = rx_ewma > min(LOAD_FRAC * rate, LOAD_ABS_MBIT)
        idle = rx_ewma < IDLE_FRAC * rate
        if loaded:
            loaded_last_t = t0
            last_loaded_rate = rate

        # -- probes: harvest finished/expired, then fire the next one
        sig = None            # worst fresh delta this tick
        for host, rtt in prober.harvest(t0):
            stats["probes"] += 1
            last_host, last_rtt = host, rtt
            if rtt is None:
                loss_streak += 1
                stats["losses"] += 1
                continue
            loss_streak = 0
            base = baselines.get(host)
            if base is None:
                base = rtt
            else:
                # asymmetric EWMA: chase minima fast, inflate slowly
                a = 0.4 if rtt < base else (0.02 if idle else 0.0)
                base = base + a * (rtt - base)
            baselines[host] = base
            d = rtt - base
            last_delta = d
            sig = d if sig is None else max(sig, d)
        prober.fire(REFLECTORS[refl_i % len(REFLECTORS)])
        refl_i += 1

        # -- control law
        if loaded:
            severe = (sig is not None and sig > RTT_SEVERE_MS) or loss_streak >= 2
            congested = severe or (sig is not None and sig > RTT_HI_MS)
            clean = sig is not None and sig < RTT_LO_MS and loss_streak == 0
            if congested and (t0 - last_cut_t) > CUT_REFRACTORY_S:
                if severe:
                    new = min(rate * CUT_FACTOR, rx_peak * DELIVERED_FRAC)
                    new = max(new, rate * SEVERE_MIN_FACTOR)
                else:
                    new = rate * CUT_FACTOR
                new = max(FLOOR_MBIT, new)
                if new < rate:
                    old = rate
                    rate = new
                    set_bw(rate)
                    last_cut_t = t0
                    stats["cuts"] += 1
                    if severe:
                        stats["severe_cuts"] += 1
                    event = "cut_severe" if severe else "cut"
                    log("CUT%s %d->%d delta=%s loss_streak=%d rx=%.0f peak=%.0f refl=%s"
                        % ("!" if severe else "", old, rate, fmt(sig), loss_streak,
                           rx_ewma, rx_peak, last_host))
                    last_logged_rate = rate
                else:
                    event = event or "cut_clamped"
            elif clean and rate < CEIL_MBIT and (t0 - last_cut_t) > GROW_HOLDOFF_S \
                    and rx_ewma > GROW_LOAD_FRAC * rate:
                rate = min(CEIL_MBIT, rate + GROW_MBIT)
                set_bw(rate)
                event = event or "grow"
        elif idle and (t0 - loaded_last_t) > IDLE_REVERT_AFTER_S:
            since = t0 - loaded_last_t - IDLE_REVERT_AFTER_S
            target = min(BASE_MBIT, last_loaded_rate * IDLE_REVERT_HEADROOM
                         + since * IDLE_CREEP_MBIT_S)
            if rate < target:
                rate = min(target, rate + IDLE_REVERT_STEP)
                set_bw(rate)
                event = event or "revert"
        stats["min_rate"] = min(stats["min_rate"], rate)

        if abs(rate - last_logged_rate) >= 100 or \
                (rate in (FLOOR_MBIT, CEIL_MBIT, BASE_MBIT) and rate != last_logged_rate):
            log("rate=%d rx=%.0f loaded=%s" % (rate, rx_ewma, loaded))
            last_logged_rate = rate

        work_ms = (time.monotonic() - t0) * 1000.0

        # -- telemetry: every tick when there's traffic/latency, else 1-in-N
        if loaded or event or rx_ewma > IDLE_FRAC * rate \
                or (sig is not None and sig > RTT_LO_MS) \
                or tick % TELEM_IDLE_EVERY == 0:
            telem([now_iso(), "%.0f" % (dt * 1000.0), "%.0f" % work_ms,
                   "%d" % rate, fmt(rx), "%.1f" % rx_ewma, "%.1f" % rx_peak,
                   fmt(last_rtt), last_host, fmt(sig),
                   "1" if loaded else "0", str(len(prober.inflight)),
                   str(loss_streak), event])

        # -- drift: someone (configctl shaper reload) reset the pipe
        if t0 - last_drift_t > DRIFT_CHECK_S:
            last_drift_t = t0
            actual = pipe_bw_mbit()
            if actual is not None and abs(actual - rate) > 5:
                log("drift: pipe=%.0f mine=%d -> reasserting" % (actual, rate))
                set_bw(rate)

        # -- heartbeat + status
        if t0 - last_heartbeat_t > HEARTBEAT_S:
            last_heartbeat_t = t0
            log("heartbeat rate=%d rx=%.0f stats=%s baselines=%s"
                % (rate, rx_ewma, stats,
                   {k: round(v, 1) for k, v in baselines.items()}))
        if tick % 10 == 0:
            try:
                with open(STATUS + ".tmp", "w") as f:
                    json.dump({"ts": now_iso(), "version": 2,
                               "rate_mbit": int(rate),
                               "rx_mbps": round(rx_ewma, 1),
                               "rx_peak_mbps": round(rx_peak, 1),
                               "loaded": loaded,
                               "last_rtt_ms": last_rtt, "reflector": last_host,
                               "delta_ms": round(last_delta, 1) if last_delta is not None else None,
                               "loss_streak": loss_streak,
                               "last_loaded_rate": int(last_loaded_rate),
                               "stats": {k: (round(v, 1) if isinstance(v, float) else v)
                                         for k, v in stats.items()},
                               "baselines": {k: round(v, 1) for k, v in baselines.items()}}, f)
                os.rename(STATUS + ".tmp", STATUS)
            except OSError:
                pass

        time.sleep(max(0.0, TICK_S - (time.monotonic() - t0)))

    prober.shutdown()
    log("shutdown: leaving rate=%d" % rate)


if __name__ == "__main__":
    main()
