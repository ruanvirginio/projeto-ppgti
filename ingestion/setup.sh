SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

kubectl create namespace ingestion

kubectl apply -f $SCRIPT_DIR/strimzi.yaml

kubectl apply -f $SCRIPT_DIR/kafka/kafka-ephemeral.yaml -n ingestion

kubectl apply -f $SCRIPT_DIR/kafka/topic.yaml -n ingestion

## Lenses box: https://lenses.io/apache-kafka-docker/
kubectl apply -f $SCRIPT_DIR/lenses.yaml

kubectl port-forward svc/lenses 8000:80 -n ingestion

kubectl port-forward svc/kafka-kafka-bootstrap 9094:9094 -n ingestion