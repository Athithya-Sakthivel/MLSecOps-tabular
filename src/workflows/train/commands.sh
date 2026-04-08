ruff check src/workflows/train --fix
export S3_BUCKET="e2e-mlops-data-681802563986"
export TRAIN_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-train-task:2026-04-08-17-36--6455acc@sha256:484a63b3e52ee1a97c33c1723a9192c9c640e8e69213da225e0d1eeabab16987"
export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
source .venv_train/bin/activate

python -m workflows.train.run register
python -m workflows.train.run train

# delete an execution by its id as input example afrr9jtjsj2fnnwnxm75
# (lsof -i:30081 >/dev/null 2>&1 || (kubectl -n flyte port-forward svc/flyteadmin 30081:81 >/dev/null 2>&1 & sleep 2)) && read -p "Execution ID: " id && flytectl config init --host=127.0.0.1:30081 --insecure --force >/dev/null 2>&1 && flytectl delete execution "$id" -p flytesnacks -d development && kubectl delete pod -n flytesnacks-development -l execution-id="$id"

