
source .venv_train_dev/bin/activate

ray start --head --port=6379 --num-cpus=4

# ray start --address='127.0.0.1:6379' --num-cpus=4
kubectl -n default port-forward svc/iceberg-rest 8181:8181

# kubectl -n mlflow port-forward svc/mlflow 5000:5000
# kubectl -n default port-forward svc/iceberg-rest 8181:8181

kubectl -n flyte port-forward svc/flyteadmin 30081:81

# aws s3 ls s3://e2e-mlops-data-681802563986/iceberg/warehouse/gold/ --recursive