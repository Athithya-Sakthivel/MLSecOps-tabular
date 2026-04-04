aws s3api list-objects-v2 \
  --bucket e2e-mlops-data-681802563986 \
  --delimiter '/' \
  --query 'CommonPrefixes[].Prefix' \
  --output text | tr '\t' '\n' | grep -v '^None$' | while read -r p1; do

    aws s3api list-objects-v2 \
      --bucket e2e-mlops-data-681802563986 \
      --prefix "$p1" \
      --delimiter '/' \
      --query 'CommonPrefixes[].Prefix' \
      --output text | tr '\t' '\n' | grep -v '^None$' | while read -r p2; do

        aws s3api list-objects-v2 \
          --bucket e2e-mlops-data-681802563986 \
          --prefix "$p2" \
          --delimiter '/' \
          --query 'CommonPrefixes[].Prefix' \
          --output text | tr '\t' '\n' | grep -v '^None$'

    done
done