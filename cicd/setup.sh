kubectl create namespace cicd

helm repo add argo https://argoproj.github.io/argo-helm
helm repo update

# install crd's [custom resources]
# argo-cd
# https://artifacthub.io/packages/helm/argo/argo-cd
# https://github.com/argoproj/argo-helm
helm upgrade --install argocd argo/argo-cd --namespace cicd --wait

kubectl port-forward svc/argocd-server 8080:80 -n cicd

ARGOCD_LB="localhost:8080"
password="$(kubectl -n cicd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)"

kubectl create clusterrolebinding cluster-admin-binding --clusterrole=cluster-admin --user=system:serviceaccount:cicd:argocd-application-controller -n cicd

argocd login localhost:8080 --username admin --password $password
CLUSTER="minikube"
argocd cluster add $CLUSTER --in-cluster

REPOSITORY="git@github.com:ifpb/big-data-pipeline.git"
argocd repo add $REPOSITORY --ssh-private-key-path ~/.ssh/id_rsa_if

## password: ekpWbZOndsinPalR