export ELT_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-29-21-09--dac97d5@sha256:a17ea492ba0d33a9d26a008477f7a6c87a54c51331ea595e3e44539932eab21d"
source .venv_elt/bin/activate
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python -m workflows.ELT.run register
python -m workflows.ELT.run elt
