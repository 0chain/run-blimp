# stopone.sh <q>: stop ONE query's run (client + gateway); the runlist moves on
Q=$1; T=$(cat /opt/0chain/zs3server/environment/admin_token)
for p in $(pgrep -f "qcheck[.]sh $Q\$"); do kill $p; done
for p in $(pgrep -f "queries $Q\$"); do kill $p; done
ids=$(curl -s -m5 http://127.0.0.1:9000/admin/query/active -H "Authorization: Bearer $T" | python3 -c "import json,sys; print(' '.join(str(r['id']) for r in json.load(sys.stdin).get('active',[]) if r['label'].split(':')[0]=='q$Q'))")
for i in $ids; do curl -s -m5 -X POST http://127.0.0.1:9000/admin/query/cancel -H "Authorization: Bearer $T" -d "{\"id\":$i}"; done
sleep 8; echo "now: $(pgrep -af 'qcheck[.]sh' | sed 's/.*qcheck.sh//')"
