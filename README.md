# adaptive-shaper

A small cake-autorate-style bandwidth controller for an OPNsense (FreeBSD/dummynet)
download shaper. A cable (DOCSIS) line's delivered rate swings over the day, so a
fixed fq_codel pipe rate is either too high (bufferbloat returns when the CMTS
becomes the bottleneck) or too low (wasted capacity). This daemon reads WAN rx
bytes from `netstat`, fires async ICMP probes at public anycast reflectors, and
retunes the dummynet pipe bandwidth at runtime with `dnctl`, which preserves the
FQ_CODEL scheduler.

## Design

- **Tick** 0.5 s. RTT probes are fire-and-forget (`Popen`), harvested each tick,
  killed after a timeout, so a lost ping never stalls the loop.
- **Baseline** per reflector: asymmetric EWMA (chase minima fast, inflate only
  slowly while idle). Signal = worst fresh `rtt - baseline` this tick.
- **Control law** (only while "loaded", i.e. rx above a fraction of the pipe rate
  or an absolute threshold):
  - delta > `RTT_HI_MS` -> multiplicative cut (`CUT_FACTOR`);
  - delta > `RTT_SEVERE_MS` or 2 consecutive probe losses -> severe cut to
    `DELIVERED_FRAC` x recent peak delivered rx (i.e. below what the upstream is
    actually handing over), bounded below by `SEVERE_MIN_FACTOR` x rate;
  - delta < `RTT_LO_MS` and pipe actually binding -> additive growth after a holdoff.
- **Idle reversion**: after `IDLE_REVERT_AFTER_S` idle, step back to the last
  loaded rate (+10%) and creep toward `BASE_MBIT`.
- **Drift check**: if something (e.g. a GUI shaper reload) resets the pipe, reassert.
- **Telemetry**: per-tick CSV (decimated when idle), JSON status file, log with
  heartbeats; all timestamps are absolute ISO-8601 wall clock.

`adaptive-shaper.py` is v2; `adaptive-shaper.py.v1` is the original synchronous
version, kept for reference.

## Deploy on OPNsense

Prereq: a Traffic Shaper download pipe with the FQ_CODEL scheduler. Set
`DOWN_PIPE` (pipe number), `IFACE` (WAN interface) and the `*_MBIT` bounds at the
top of the script to match your line.

```sh
scp adaptive-shaper.py  root@firewall:/usr/local/sbin/adaptive-shaper.py
scp adaptive_shaper.rc  root@firewall:/usr/local/etc/rc.d/adaptive_shaper
ssh root@firewall 'chmod +x /usr/local/sbin/adaptive-shaper.py /usr/local/etc/rc.d/adaptive_shaper;
  sysrc adaptive_shaper_enable=YES; service adaptive_shaper start'
tail -f /var/log/adaptive_shaper.log    # on the firewall
cat /var/run/adaptive_shaper.status
```

`adload.sh` is a crude multi-stream download load generator (Ookla servers) for
exercising the controller.

License: MIT.
