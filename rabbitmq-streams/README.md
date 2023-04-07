# README 

Run this first: 

```shell
docker compose up 
docker ps // get the CONTAINER_ID
docker exec $CONTAINER_ID  rabbitmq-plugins enable rabbitmq_stream 
```