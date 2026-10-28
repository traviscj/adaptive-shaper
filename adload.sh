#!/bin/sh
hosts=$(curl -s "https://www.speedtest.net/api/js/servers?engine=js&search=Chicago&limit=10" \
  | python3 -c 'import sys,json; [print(s["host"].split(":")[0]) for s in json.load(sys.stdin)[:6]]')
start=$(date +%s)
for h in $hosts; do
  for i in 1 2; do
    curl -sk -o /dev/null -w "%{size_download} " -m 45 "http://$h:8080/download?size=4000000000" &
  done
done > /root/adload.bytes
wait
end=$(date +%s)
total=$(tr ' ' '\n' < /root/adload.bytes | awk '{s+=$1} END {print s}')
echo "seconds=$((end-start)) bytes=$total mbps=$(echo "$total $((end-start))" | awk '{printf "%.0f", $1*8/$2/1e6}')"
