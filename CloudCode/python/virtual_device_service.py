from kafka import KafkaConsumer, KafkaProducer
from const import *
import threading

from concurrent import futures
import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

# Twin state
dispositivos = [
    {
        'nome':'sensor_temperatura',
        'porta_fisica':None,
        'estado':0
    },
    {
        'nome':'sensor_luminosidade',
        'porta_fisica':29,
        'estado':0
    },
    {
        'nome':'led_vermelho',
        'porta_fisica':16,
        'estado':0
    },
    {
        'nome':'led_verde',
        'porta_fisica':18,
        'estado':0
    }
]

# Users
users = [
    {
        'username':'usuario1',
        'password':'senha1'
    },
    {
        'username':'usuario2',
        'password':'senha2'
    },
    {
        'username':'usuario3',
        'password':'senha4'
    },
]

# Kafka consumer to run on a separate thread
def consume_temperature():
    sensor = next(disp for disp in dispositivos 
                    if disp['nome'] == 'sensor_temperatura' 
                    and disp['porta_fisica'] == None)
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print ('Received Temperature: ', msg.value.decode())
        sensor['estado'] = msg.value.decode()

# Kafka consumer to run on a separate thread
def consume_light_level():
    sensor = next(disp for disp in dispositivos 
                    if disp['nome'] == 'sensor_luminosidade' 
                    and disp['porta_fisica'] == 29)
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        print ('Received Light Level: ', msg.value.decode())
        sensor['estado'] = msg.value.decode()

def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state
        
class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        sensor = next(disp for disp in dispositivos 
                    if disp['nome'] == 'sensor_temperatura' 
                    and disp['porta_fisica'] == None)
        return iot_service_pb2.TemperatureReply(temperature=sensor['estado'])
    
    def BlinkLed(self, request, context):
        print ("Blink led ", request.ledname)
        print ("...with state ", request.state)
        produce_led_command(request.state, request.ledname)
        # Update led state of twin
        if request.name == 'red':
            led = next(disp for disp in dispositivos 
                    if disp['nome'] == 'led_vermelho' 
                    and disp['porta_fisica'] == 16)
        else:
            led = next(disp for disp in dispositivos 
                    if disp['nome'] == 'led_verde' 
                    and disp['porta_fisica'] == 18)
        led['estado'] = request.state
        return iot_service_pb2.LedReply(ledstate={'red': dispositivos[2]['estado'], 
                                                'green':dispositivos[3]['estado']})

    def SayLightLevel(self, request, context):
        sensor = next(disp for disp in dispositivos 
                    if disp['nome'] == 'sensor_luminosidade' 
                    and disp['porta_fisica'] == 29)
        return iot_service_pb2.LightLevelReply(lightLevel=sensor['estado'])

    def Login(self, request, context):
        for user in users:
            if user['username'] == request.username and \
                user['password'] == request.password:
                return iot_service_pb2.StatusReply(status='OK')
            else:
                return iot_service_pb2.StatusReply(status='NOT OK')
        

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
    led_state = {'red':dispositivos[2]['estado'], 'green':dispositivos[3]['estado']}
    for color in led_state.keys():
        produce_led_command (led_state[color], color)
    serve()
