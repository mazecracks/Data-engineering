from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:29092'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'Message failed: {err}')
    else:
        print(
            f'Message delivered to {msg.topic()} '
            f'[partition {msg.partition()}] '
            f'at offset {msg.offset()}'
        )

producer.produce(
    topic='test-topic',
    key='key1',
    value='hello kafka!!!',
    on_delivery=delivery_report
)

producer.flush()
