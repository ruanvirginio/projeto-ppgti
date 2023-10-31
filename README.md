Subindo ambiente do minikube 

Pré-requisitos:
* Git Instalado (https://git-scm.com/book/pt-br/v2/Começando-Instalando-o-Git)
* Docker desktop instalado (windows: https://docs.docker.com/desktop/install/windows-install, linux: https://docs.docker.com/desktop/install/linux-install/)
* Minikube instalado (https://minikube.sigs.k8s.io/docs/start/)
* Helm instalado (https://helm.sh/docs/intro/install/)
* Instale o Argo CLI (https://github.com/argoproj/argo-cd/blob/master/docs/cli_installation.md) (Adapte pro seu caminho, aqui to usando pro Linux)

```
curl -sSL -o argocd-linux-amd64 https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64
sudo install -m 555 argocd-linux-amd64 /usr/local/bin/argocd
rm argocd-linux-amd64
```

Com o Docker desktop rodando, abra o terminal e execute o comando:

minikube start --driver=docker --cpus=2 --memory=3863

* Ajuste a quantidade de memória/cpu de acordo com o disponível na sua máquina


## Fazendo clone do projeto Big data pipeline
Rode o comando 
```
git clone https://github.com/ruanvirginio/projeto-ppgti.git
```

## Subindo o Argo (CI/CD)
1. Abra o projeto big-data-pipeline no terminal e entre no diretório "cicd". 
2. Crie o namespace "cicd" no cluster kubernetes do Minikube:
```
kubectl create namespace cicd
```

3. Atualize os repositórios do helm e adicione o repositório de charts do Argo ao helm local
```
helm repo add argo https://argoproj.github.io/argo-helm
helm repo update
```

4. Use o comando helm install para instalar o Argo a partir do seu chart.
```
helm upgrade --install argocd argo/argo-cd --namespace cicd --wait
```

5. Faça port-forward do argo para poder ter acesso à sua interface gráfica via navegador:
```
kubectl port-forward svc/argocd-server 8080:80 -n cicd
```

6. Em uma nova aba do terminal, rode o seguinte comando para descobrir a senha de acesso (usuário padrão é admin):
```
password="$(kubectl -n cicd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)"
echo $password
```

7. Adicione a permissão ao Argo para gerenciar recursos no cluster:
```
kubectl create clusterrolebinding cluster-admin-binding --clusterrole=cluster-admin --user=system:serviceaccount:cicd:argocd-application-controller -n cicd
```

8. Adicione o cluster do minikube como cluster gerenciado pelo argo
```
argocd login localhost:8080 --username admin --password $password
CLUSTER="minikube"
argocd cluster add $CLUSTER --in-cluster
```

9. Adicione o repositório big-data-pipeline como gerenciado pelo argo. Isso fará com que o Argo aplique automaticamente atualizações enviadas ao repositório.
```
REPOSITORY="git@github.com:ruanvirginio/projeto-ppgti.git"
argocd repo add $REPOSITORY --ssh-private-key-path ~/.ssh/id_rsa
```

> Acesse a interface do argo em http://localhost:8080 e faça login com usuário admin e senha contida na variável $password

## Subindo o Minio (Datalake)

1. Crie o namespace "datalake" no cluster kubernetes do Minikube:
```
kubectl create namespace datalake
```
2. Aplique a configuração de nova aplicação do argo para subir o minio:
```
kubectl apply -f datalake/minio.yaml
```

3. Aguarde a aplicação ser sua instalação finalizada (acompanhe via Argo) e em seugida faça port-forward do minio para poder ter acesso à sua interface gráfica via navegador. 
A porta 9001 será usada para acessar o console (via navegador), enquanto que a porta 9000 será usada para acessar o minio via código (API).

```
kubectl port-forward svc/minio 9000:9000 -n datalake & kubectl port-forward svc/minio-console 9001:9001 -n datalake &
```

> Acesse a interface do minio em http://localhost:9001 e faça login com usuário root e senha minio123

## Subindo Airflow (Orchestration)
1. Crie o namespace "orchestration" no cluster kubernetes do Minikube:
```
kubectl create ns orchestrator
```
2. Aplique a configuração de nova aplicação do argo para subir o airflow:
```
kubectl apply -f airflow/airflow.yaml
```

3. Aguarde a aplicação ser sua instalação finalizada (acompanhe via Argo) e em seguida faça port-forward do airflow para poder ter acesso à sua interface gráfica via navegador.
```
kubectl port-forward svc/airflow-web 8001:8080 -n orchestrator
```
