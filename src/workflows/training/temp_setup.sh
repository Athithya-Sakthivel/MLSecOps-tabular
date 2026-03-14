kubectl -n default port-forward svc/iceberg-rest 9001:9001 &
ray start --head --num-cpus 4 --disable-usage-stats