from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
from google.oauth2 import service_account

def bq_api() -> None:
    pass

def pull_messages(consumer, topic='teamai.btc_price_training') -> None:
    consumer.subscribe([topic])
    while True:
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                msg_prnt = f'''
                    Successfully poll a record from 
                    Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}
                    message key: {message.key()} || message value: {message.value()}
                '''
                print(msg_prnt)
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")
    else:
        consumer.close()


if __name__ == "__main__":
    consumer_config = {
        "bootstrap.servers": "broker:29092",
        "schema.registry.url": "http://schema-registry:8081",
        "group.id": "teamai.btcprice.avro.consumer.1712",
        "auto.offset.reset": "earliest"
    }

    ####### Kafka Avro Consumer #######
    consumer = AvroConsumer(consumer_config)
    ###################################

    pull_messages(consumer)
