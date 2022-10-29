#kafka_assignment_produce

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd 
from typing import List
import sys 



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



def get_restaurent_instance(FILE_PATH):
    df_restaurent=pd.read_csv(FILE_PATH)
    # df_restaurent=df_restaurent.iloc[:,1:]

    rest:List[restaurent_order]=[]
    for data in df_restaurent.values:
        order_data=restaurent_order(dict(zip(columns_name,data)))
        rest.append(order_data)
        yield order_data    



def restaurent_dict(order:restaurent_order, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return order.record


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
	string_serializer = StringSerializer('utf_8')
	json_serializer = JSONSerializer(schema_str, schema_registry_client, restaurent_dict)

	producer = Producer(sasl_conf())

	print("Producing user records to topic {}. ^C to exit.".format(topic))
	#while True:
	# Serve on_delivery callbacks from previous calls to produce()
	producer.poll(0.0)
	try:
	    for restaurent in get_restaurent_instance(FILE_PATH=file_path):

	        print(restaurent)
	        producer.produce(topic=topic,
	                      		key=string_serializer(str(uuid4()), restaurent_dict),
	                            value=json_serializer(restaurent, SerializationContext(topic, MessageField.VALUE)),
	                            on_delivery=delivery_report)
	        break
	except KeyboardInterrupt:
	    pass
	except ValueError:
	    print("Invalid input, discarding record...")
	    pass

	    print("\nFlushing records...")
	producer.flush()

main_topic("restaurent-take-away-data")







	









