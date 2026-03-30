export ELT_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-30-07-33--15e04f8"
source .venv_elt/bin/activate
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python -m workflows.ELT.run register
python -m workflows.ELT.run elt