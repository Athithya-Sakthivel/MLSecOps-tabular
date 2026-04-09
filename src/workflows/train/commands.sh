ruff check src/workflows/train --fix
export S3_BUCKET="e2e-mlops-data-681802563986"
export TRAIN_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-train-task:2026-04-08-19-47--d0676be@sha256:sha256:b9e0809c519c543f3414eb88b95f89ebb8cc5b63c245b97fa0402e3f1d3523d5"
export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
source .venv_train/bin/activate

python -m workflows.train.run register
python -m workflows.train.run train

# delete an execution by its id as input example afrr9jtjsj2fnnwnxm75
# (lsof -i:30081 >/dev/null 2>&1 || (kubectl -n flyte port-forward svc/flyteadmin 30081:81 >/dev/null 2>&1 & sleep 2)) && read -p "Execution ID: " id && flytectl config init --host=127.0.0.1:30081 --insecure --force >/dev/null 2>&1 && flytectl delete execution "$id" -p flytesnacks -d development && kubectl delete pod -n flytesnacks-development -l execution-id="$id"
