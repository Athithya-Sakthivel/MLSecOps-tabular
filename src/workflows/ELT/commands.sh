bash -lc 'set -Eeuo pipefail
source .venv_elt/bin/activate
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
export ELT_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-29-07-26--4162406@sha256:79ab860f821f3d26a08ab9f4c53e19c5ef63d42e93c4cd2d2b00d4f9b6d160f8"
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
