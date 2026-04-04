core:
	kind delete cluster --name local-cluster || true && kind create cluster --name local-cluster && \
	K8S_CLUSTER=kind bash src/infra/core/default_storage_class.sh && \
	K8S_CLUSTER=kind bash src/infra/core/postgres_cluster.sh deploy

elt:
	bash src/infra/elt/iceberg.sh --rollout && bash src/infra/elt/spark_operator.sh --rollout && \
	python3 src/infra/core/flyte_setup.py --rollout

prune-elt:
	bash 

backup-pg:
	bash src/infra/core/postgres_cluster.sh backup && aws s3 ls $$PG_BACKUPS_S3_BUCKET/postgres_backups/ --recursive

train:
	bash src/infra/train/kuberay_operator.sh --rollout && python3 src/infra/train/mlflow_server.py


set-sa:
	bash src/core/default_storage_class.sh

tree:
	tree -a -I '.git|.venv|.repos|__pycache__|venv'

push:
	git add .
	git commit -m "new"
	gitleaks detect --source . --exit-code 1 --redact
	git push origin main --force

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.log" ! -path "./.git/*" -delete
	find . -type f -name "*.pulumi-logs" ! -path "./.git/*" -delete
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf logs
	rm -rf src/terraform/.plans
	clear

recreate:
	make rollout-pg && bash src/tests/core/postgres_cluster.sh || true

rollout-signoz:
	bash src/core/signoz.sh --rollout && bash src/tests/signoz.sh


validate-pg:
	kind delete cluster --name local-cluster || true && kind create cluster --name local-cluster && \
	K8S_CLUSTER=kind bash src/infra/core/default_storage_class.sh && \
	K8S_CLUSTER=kind bash src/infra/core/postgres_cluster.sh deploy && \
	bash src/tests/infra/validate_cnpg_latest_restore.sh && \
	bash src/tests/infra/validate_cnpg_PITR.sh && \
	aws s3 ls s3://$$PG_BACKUPS_S3_BUCKET/postgres_backups/ --recursive



iac-staging:
	bash src/terraform/run.sh --create --env staging || true
delete-iac-staging:
	bash src/terraform/run.sh --delete --yes-delete --env staging

test-iac-staging:
	bash src/terraform/run.sh --create --env staging || true && \
	bash src/terraform/run.sh --delete --yes-delete --env staging

sync:
	aws s3 sync s3://$$S3_BUCKET/iceberg/warehouse/ $(pwd)/data/iceberg/

set-staging-eks-context:
	./src/scripts/set_k8s_context.sh staging

set-prod-eks-context:
	./src/scripts/set_k8s_context.sh prod

set-kind-context:
	kubectl config use-context kind-rag8s-local


delete-cloudflared-agents:
	python3 infra/generators/cloudflared.py --delete --namespace inference || true

cloudflare-setup:
	bash infra/setup/cloudflared.sh

cloudflare-logout:
	rm -rf ~/.cloudflared && \
	rm -f ~/.config/rag/secrets.env && \
	unset CLOUDFLARE_TUNNEL_TOKEN && \
	unset CLOUDFLARE_TUNNEL_CREDENTIALS_B64 && \
	unset CLOUDFLARE_TUNNEL_NAME

