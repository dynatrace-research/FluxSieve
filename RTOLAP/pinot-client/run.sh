
#!/bin/bash

# Normalize prefix (remove trailing slashes and add one)
BASE_PREFIX="pinot-results/40M-logs_4-servers-lowerselectivity" #20M-logs_4-servers-lowerselectivityQ2-extra
BASE_PREFIX="${BASE_PREFIX%/}/"

TOTAL_RUNS=10
for run in $(seq 2 $TOTAL_RUNS); do
    echo "=== Starting run $run ==="

    # Export environment variables
    export S3_KEY_PREFIX="$BASE_PREFIX"
    export CSV_FILENAME="40M-logs-all-run${run}.csv"

    echo "Using S3_KEY_PREFIX: $S3_KEY_PREFIX"
    echo "Using CSV_FILENAME: $CSV_FILENAME"


    # Apply deployment
    envsubst < deployment-lowselectivity.yaml | kubectl apply -f -

    # Wait for pods to start
    echo "Waiting 360 seconds for pods to run..." #5M 40s #360 for 40M

    sleep 360

    echo "deleting deployment..."

    kubectl delete -f deployment-lowselectivity.yaml

    sleep 1

    (kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep '^pinot-server-' | xargs kubectl delete pod) > /dev/null 2>&1 &

    echo "Pod deletion started in background with PID $!" 

    echo "Waiting 350 seconds for pods to restart..." #5M 70s #350 for 40M

    sleep 350

    export S3_KEY_PREFIX="$BASE_PREFIX"
    export CSV_FILENAME="40M-logs-all-run${run}-count-queries.csv"

    echo "Using S3_KEY_PREFIX: $S3_KEY_PREFIX"
    echo "Using CSV_FILENAME: $CSV_FILENAME"


    # Apply deployment
    envsubst < deployment-lowselectivity-count.yaml | kubectl apply -f -

    # Wait for pods to start
    echo "Waiting 260 seconds for pods to run..." #5M 30s #260 for 40M
    sleep 260

    echo "deleting deployment..."
    kubectl delete -f deployment-lowselectivity-count.yaml
    sleep 1

    (kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep '^pinot-server-' | xargs kubectl delete pod) > /dev/null 2>&1 &

    echo "Pod deletion started in background with PID $!"

    echo "Waiting 350 seconds for pods to restart..." #350 for 40M
    sleep 350
done
echo "All $TOTAL_RUNS runs completed."