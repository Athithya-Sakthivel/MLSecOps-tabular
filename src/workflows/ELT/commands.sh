export ELT_TASK_IMAGE="ghcr.io/athithya-sakthivel/flyte-elt-task:2026-03-28-16-58--ab2a8d6"
source .venv_elt/bin/activate
python3 -m workflows.ELT.run register && python -m workflows.ELT.run elt