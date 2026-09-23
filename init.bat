docker build -t rodperoba/factory-producer:latest ./Code/Produtor
docker build -t rodperoba/factory-consumer:latest ./Code/Consumidor

docker push rodperoba/factory-producer:latest
docker push rodperoba/factory-consumer:latest

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/mysql.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/producer.yaml
kubectl apply -f k8s/consumer.yaml