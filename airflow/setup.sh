kubectl create ns orchestrator
kubectl apply -f airflow.yaml

kubectl port-forward svc/airflow-web 8001:8080 -n orchestrator