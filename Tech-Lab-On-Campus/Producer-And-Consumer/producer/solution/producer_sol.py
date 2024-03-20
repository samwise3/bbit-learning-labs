import os
import pika
from producer_interface import mqProducerInterface


class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str):
        # Call the parent class constructor to ensure any initialization there is done
        super().__init__(routing_key, exchange_name)
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        # Create the exchange if not already present, considering it as a direct exchange for simplicity
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

    def publishOrder(self, message):
        self.channel.basic_publish(exchange=self.exchange_name,
                                   routing_key=self.routing_key,
                                   body=message,
                                   properties=pika.BasicProperties(
                                       delivery_mode=2,  # make message persistent
                                   ))
        print(f" [x] Sent '{message}'")
        self.closeConnection()

    def closeConnection(self):
        self.channel.close()
        self.connection.close()

