ruff check src/workflows/ELT --fix
source .venv_elt/bin/activate
# aws s3 ls s3://$S3_BUCKET/iceberg/warehouse --recursive && kubectl -n flyte port-forward svc/flyteadmin 30081:81
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
K8S_CLUSTER=kind python -m workflows.ELT.run elt # schedule

# delete an execution by its id as input
# (lsof -i:30081 >/dev/null 2>&1 || (kubectl -n flyte port-forward svc/flyteadmin 30081:81 >/dev/null 2>&1 & sleep 2)) && read -p "Execution ID: " id && flytectl config init --host=127.0.0.1:30081 --insecure --force >/dev/null 2>&1 && flytectl delete execution "$id" -p flytesnacks -d development && kubectl delete pod -n flytesnacks-development -l execution-id="$id"
