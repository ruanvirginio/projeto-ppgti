SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
kubectl create namespace datalake
kubectl apply -f $SCRIPT_DIR/minio.yaml

kubectl port-forward svc/minio-console 9001:9001 -n datalake
