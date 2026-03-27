cd /workspace/src/scripts/archive
echo $GIT_PAT | docker login ghcr.io -u athithya-sakthivel --password-stdin
docker build -t ghcr.io/athithya-sakthivel/trivy-0.69.3-gitleaks-8.30.1-opengrep-1.16.5:v3 .
docker push ghcr.io/athithya-sakthivel/trivy-0.69.3-gitleaks-8.30.1-opengrep-1.16.5:v3 
