# stopone.sh <q>: stop ONE query's run (client + tick loop + gateway); the runlist moves on
# <q>: a TPC query number (17) or a SQL file's name (v_q67_having).
# A run is tpc_batch -> blimp --query ... -> bench_cdc.sh (the tick loop) -> curl/python.
# Killing blimp alone orphans bench_cdc.sh: it keeps re-issuing /admin/query/run and
# holds tpc_batch's log pipe open, so the list never moves on (2026-11-10). Every
# process of the query is found by PID — blimp/qcheck by args, bench_cdc.sh by its
# SQL_FILES / QNRS env (finds an already-orphaned loop too) — and its tree is killed.
Q=$1; T=$(cat /opt/0chain/zs3server/environment/admin_token)
N=${Q#q}; case "$N" in ''|*[!0-9]*) N="" ;; esac
# qpids: root PIDs of every process of this query's run
qpids() {
  pgrep -f "qcheck[.]sh $Q\$"
  pgrep -f "queries $Q\$"
  pgrep -f "blimp --query --sql [^ ]*/$Q[.]sql"
  [ -z "$N" ] || pgrep -f "blimp --query --tpc $N( |\$)"
  for p in $(pgrep -f "bench_cdc[.]sh"); do
    e=$(tr '\0' '\n' < /proc/$p/environ 2>/dev/null)
    sf=$(echo "$e" | sed -n 's/^SQL_FILES=//p'); qn=$(echo "$e" | sed -n 's/^QNRS=//p')
    if [ -n "$sf" ]; then for f in $sf; do [ "$(basename "$f" .sql)" = "$Q" ] && echo $p; done
    elif [ -n "$N" ] && [ "$qn" = "$N" ]; then echo $p; fi
  done
}
# kt <pid> [-9]: kill the parent first (no new children), then its children's trees
kt() { local c; c=$(pgrep -P $1); kill $2 $1 2>/dev/null; for x in $c; do kt $x $2; done; }
for p in $(qpids | sort -u); do kt $p; done
ids=$(curl -s -m5 http://127.0.0.1:9000/admin/query/active -H "Authorization: Bearer $T" | python3 -c "import json,sys; print(' '.join(str(r['id']) for r in json.load(sys.stdin).get('active',[]) if r['label'].split(':')[0] in ('q$Q','$Q','q$N')))")
for i in $ids; do curl -s -m5 -X POST http://127.0.0.1:9000/admin/query/cancel -H "Authorization: Bearer $T" -d "{\"id\":$i}"; done
sleep 8
# separate check: anything of the query still alive gets SIGKILL, then is checked again
left=$(qpids | sort -u | tr '\n' ' ')
if [ -n "${left% }" ]; then for p in $left; do kt $p -9; done; sleep 2; left=$(qpids | sort -u | tr '\n' ' '); fi
if [ -n "${left% }" ]; then echo "STILL RUNNING for $Q: $left"; ps -o pid,ppid,etimes,args -p "$(echo $left | tr ' ' ,)"; exit 1; fi
echo "stopped $Q: no process left"
echo "now: $(pgrep -af 'blimp --query' | sed 's/.*blimp --query//')"
