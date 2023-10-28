# Apache Kafka Security with Strimzi Operator

### security options for strimzi operator
```shell
# links
# https://docs.confluent.io/platform/current/security/security_tutorial.html
# https://docs.confluent.io/platform/current/kafka/authentication_ssl.html
# https://docs.confluent.io/platform/current/kafka/encryption.html#kafka-ssl-encryption
# https://github.com/strimzi/strimzi-kafka-operator/tree/main/examples/security
# https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_scram.html

# strimzi security [options]
tls-auth
scram-sha-512-auth
keycloak-authorization

# type = scram-sha-512-auth
# username & password authentication 
# using tls - secure channel
```

```shell
# https://strimzi.io/documentation/
# https://github.com/strimzi/strimzi-kafka-operator/tree/0.25.0/examples/security
# https://strimzi.io/docs/operators/latest/overview.html#security-configuration-authentication_str

# deploy kafka broker using [tls]
# /Users/luanmorenomaciel/BitBucket/applications/ingestion-data-stores-app/deployment
k apply -f kafka-ephemeral.yaml

# verify operator [log]
k logs strimzi-cluster-operator-687fdd6f77-gv6k6 -f

# retrieve the broker status
kubectl get kafka -o yaml

# deploy topic and user
k apply -f topic.yaml
k apply -f user.yaml

# search for topics and registered users
k get kafkatopics
k get kafkausers

# retrieve secrets ~ users
# user [cert] and user [key] 
k get secrets diego.pessoa -o yaml

# retrieve the kafka certificate
# edh-cluster-ca-cert
k get secrets edh-cluster-ca-cert -o yaml

# retrieve password and ca cert
k get secrets/diegopessoa --template={{.data.password}} -n ingestion | base64 -D 
k get secret kafka-cluster-ca-cert -o jsonpath='{.data.ca\.crt}' -n ingestion | base64 -d > /Users/diegopessoa/projects/ifpb/integracao-dados/big-data-pipeline/ingestion/kafka/ca.crt

# install openssl on [macos]
# identify certificate information
brew install openssl
openssl version -a

# trigger ingestion
python3.9 cli.py 'strimzi-users-json-ssl'
```