#kafka_assignment_consumer program

import argparse
from confluent_kafka import Consumer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import csv 
import pandas as pd
import os



file_path = "D:/big_data/Confluent-Kafka-Setup-main/Confluent-Kafka-Setup-main/restaurant_orders1.csv"
# columns_name = ['Order Number',	'Order Date', 'Item Name', 'Quantity', 'Product Price', 'Total products']
columns_name = ['Order_Number','Order_Date','Item_Name','Quantity','Product_Price','Total_products']

api_key = 'MACM3QZZOQKLYDKF'
api_secret_key = 'x4YwJOV4TrgcFK4FRF+v8mJcuFpxE+Ql1xoNcaOH53yVhadn4ciY7LkQ0ock3dGn'
bootstrap_server = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
schema_registry_key = 'BGTRCJQFH5N6X5UJ'
schema_api_secret_key = 'lWBPp+XixzB+lBd95VKPua1ItGcJ3WgaC9O0wrRGxCaZ0O05Ea2QsQSswIbFwVE1'
security_protocol = 'SASL_SSL'
ssl_machenism = 'PLAIN'
end_point_schema_url = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'


def  sasl_conf():

	sasl_conf = {  

	'sasl.username' : api_key, 
	'sasl.password' : api_secret_key, 
	'bootstrap.servers' : bootstrap_server,
	'sasl.mechanism' : ssl_machenism,
	'security.protocol' :security_protocol 
	}

	return sasl_conf


def schema_conf():

	schema_configuration = { 'url' : end_point_schema_url,

	'basic.auth.user.info' : f"{schema_registry_key}:{schema_api_secret_key}"
						}
	return schema_configuration


class restaurent_order():

	def __init__(self,record:dict):
		for k,v in record.items():
			setattr(self,k,v)
		
		self.record = record


	@staticmethod
	def dict_to_restaurent(data:dict,ctx):
		return restaurent_order(record=data)

	def __str__(self):
		return f"{self.record}"


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))




def main_topic(topic):

	schema_registry_conf = schema_conf()
	schema_registry_client = SchemaRegistryClient(schema_registry_conf)
	schema_str = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str


	json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=restaurent_order.dict_to_restaurent)
	consumer_conf = sasl_conf()
	consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

	consumer = Consumer(consumer_conf)
	consumer.subscribe([topic])

	message = []
	while True:
		try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
			msg = consumer.poll(1.0)
			if msg is None:
				if len(message) != 0:
					df = pd.DataFrame.from_records(message)
					if not os.path.isfile('output.csv'):
						df.to_csv("output.csv",index = False)
					else:
						df.to_csv('output.csv', mode='a', header=False, index=False)
					message = []	
				continue

			restaurent_ord = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))				
			if restaurent_ord is not None:
				print(restaurent_ord)
				#print("User record {}: order: {}\n"
                 #    .format(msg.key(), restaurent_ord))
			
		except KeyboardInterrupt:
			print(KeyboardInterrupt)
			break
	consumer.close()
main_topic("restaurent-take-away-data")






