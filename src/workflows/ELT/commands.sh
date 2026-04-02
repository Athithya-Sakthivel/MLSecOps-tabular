source .venv_elt/bin/activate
export ELT_PROFILE="staging"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python -m workflows.ELT.run register
# python -m workflows.ELT.run elt
python -m workflows.ELT.run elt





