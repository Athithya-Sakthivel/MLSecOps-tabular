source .venv_elt/bin/activate
# aws s3 ls s3://$S3_BUCKET/iceberg/warehouse --recursive && kubectl -n flyte port-forward svc/flyteadmin 30081:81
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"

python -m workflows.ELT.run register
python -m workflows.ELT.run elt_workflow
python -m workflows.ELT.run iceberg_maintenance_daily_lp
python -m workflows.ELT.run iceberg_maintenance_weekly_lp

# python -m workflows.ELT.run iceberg_maintenance_daily_lp --force-immediate-run
# python -m workflows.ELT.run iceberg_maintenance_weekly_lp --force-immediate-run
