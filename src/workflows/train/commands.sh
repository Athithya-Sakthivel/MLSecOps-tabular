export S3_BUCKET=e2e-mlops-data-681802563986
source .venv_train/bin/activate
export TRAIN_PROFILE="${TRAIN_PROFILE:-staging}"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python -m workflows.train.run register
python -m workflows.train.run train





