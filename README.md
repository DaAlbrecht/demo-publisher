# demo-publisher

## Start RabbitMQ

```bash
docker run -it --rm --name rabbitmq -p 5552:5552 -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management
```

## Start publisher 

```bash
git clone git@github.com:DaAlbrecht/demo-publisher.git 
cd demo-publisher
cargo run
```

This will publish a new message to the "demo" queue each second with a timestamp, some random payload and a uid `x-stream-transaction-id` transaction header.

