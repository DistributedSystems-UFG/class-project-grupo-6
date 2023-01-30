from kafka import KafkaConsumer, KafkaProducer
from const import *
import threading

from concurrent import futures
import logging
import json
import grpc
import iot_service_pb2
import iot_service_pb2_grpc

# Twin state
dispositivos = [
    {
        'id':'tem1',
        'nome':'sensor_temperatura',
        'porta_fisica':None,
        'estado':0
    },
    {
        'id':'lum1',
        'nome':'sensor_luminosidade',
        'porta_fisica':29,
        'estado':0
    },
    {
        'id':'led1',
        'nome':'led_vermelho',
        'porta_fisica':16,
        'estado':0
    },
    {
        'id':'led2',
        'nome':'led_verde',
        'porta_fisica':18,
        'estado':0
    }
]

# Users
users = [
    {
        'id': 'usr1',
        'username':'usuario1',
        'password':'senha1',
        'dispositivos': ['temp1']
    },
    {
        'id': 'usr2',
        'username':'usuario2',
        'password':'senha2',
        'dispositivos': ['lum1']
    },
    {
        'id': 'usr3',
        'username':'usuario3',
        'password':'senha3',
        'dispositivos': ['led1']
    },
    {
        'id': 'usr4',
        'username':'usuario4',
        'password':'senha4',
        'dispositivos': ['lum1','led2']
    }
]

# Kafka consumer to run on a separate thread
def consume_temperature():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        valor = msg.value
        try:
            sensor = next(disp for disp in dispositivos if disp['id'] == valor['id'])
        except StopIteration:
            sensor = None
        if sensor == None:
            dispositivos.append(valor)
            print('Device ' + valor['id'] + ' added')
        else:
            sensor['estado'] = valor['estado']
            print ('Received Temperature: ', sensor['estado'])
        

# Kafka consumer to run on a separate thread
def consume_light_level():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        valor = msg.value
        try:
            sensor = next(disp for disp in dispositivos if disp['id'] == valor['id'])
        except StopIteration:
            sensor = None
        if sensor == None:
            dispositivos.append(valor)
            print('Device ' + valor['id'] + ' added')
        else:
            sensor['estado'] = valor['estado']
            print ('Received Light Level: ', sensor['estado'])

def produce_led_command(led):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('ledcommand', led)
        
class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        user = next(usr for usr in users if usr['id'] == request.userId)
        if (request.sensorId in user['dispositivos']):
            sensor = next(disp for disp in dispositivos if disp['id'] == request.sensorId)
            return iot_service_pb2.TemperatureReply(temperature=str(sensor['estado']))
        else:
            return iot_service_pb2.TemperatureReply(temperature='ACCESS DENIED')
    
    def BlinkLed(self, request, context):
        user = next(usr for usr in users if usr['id'] == request.userId)
        if (request.sensorId in user['dispositivos']):
            print ("Blink led ", request.ledname)
            print ("...with state ", request.state)
            # Update led state of twin
            led = next(disp for disp in dispositivos if disp['id'] == request.ledId)
            led['estado'] = request.state
            produce_led_command(led)
            return iot_service_pb2.LedReply(ledstate=str(led['estado']))
        else:
            return iot_service_pb2.LedReply(ledstate='ACCESS DENIED')

    def SayLightLevel(self, request, context):
        user = next(usr for usr in users if usr['id'] == request.userId)
        if (request.sensorId in user['dispositivos']):
            sensor = next(disp for disp in dispositivos if disp['id'] == request.sensorId)
            return iot_service_pb2.LightLevelReply(lightLevel=str(sensor['estado']))
        else:
            return iot_service_pb2.LightLevelReply(lightLevel='ACCESS DENIED')

    def Login(self, request, context):
        for user in users:
            if user['username'] == request.username and \
                user['password'] == request.password:
                return iot_service_pb2.LoginReply(userId=user['id'])
        return iot_service_pb2.LoginReply(userId=None)     
        

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()

    trd1 = threading.Thread(target=consume_temperature)
    trd1.start()

    trd2 = threading.Thread(target=consume_light_level)
    trd2.start()

    # Initialize the state of the leds on the actual device
    for dispositivo in dispositivos:
        if dispositivo['id'].startswith('led'):
            produce_led_command(dispositivo)
    serve()
