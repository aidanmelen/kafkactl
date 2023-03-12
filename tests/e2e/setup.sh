kubectl create namespace confluent
helm upgrade --install confluent-operator confluentinc/confluent-for-kubernetes --namespace confluent 
kubectl apply -f confluent-platform.yaml