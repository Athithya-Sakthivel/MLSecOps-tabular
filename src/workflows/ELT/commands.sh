bash -lc 'set -Eeuo pipefail
source .venv_elt/bin/activate
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
export ELT_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-29-20-19--18b29cc@sha256:294972362d9cbab2e0cb65a539f8ff1def0d0059641fba531cdc6099e4656787"
python -m workflows.ELT.run register
python -m workflows.ELT.run elt

echo "== SPARK APPLICATIONS =="
kubectl -n flytesnacks-development get sparkapplications -o wide || true

echo "== DRIVER PODS =="
kubectl -n flytesnacks-development get pods -l spark-role=driver -o wide || true

echo "== EXECUTOR PODS =="
kubectl -n flytesnacks-development get pods -l spark-role=executor -o wide || true

echo "== RECENT EVENTS =="
kubectl -n flytesnacks-development get events --sort-by=.lastTimestamp | tail -n 3 || true

echo "== OPERATOR LOG TAIL =="
kubectl -n spark-operator logs deploy/spark-operator-controller --tail=5 || true
'
