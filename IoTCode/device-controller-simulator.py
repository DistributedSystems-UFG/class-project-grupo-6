import time
from kafka import KafkaProducer, KafkaConsumer
import math
import json
import threading
from random import uniform
from random import randint
from const import *
from datetime import datetime

dispositivos = [
    {
        'id':'tem1',
        'nome':'sensor_temperatura',
        'porta_fisica':None,
        'estado': []
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

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def get_datetime():
    now = datetime.now()
    iso_date = now.isoformat()
    return str(iso_date)

def read_temp():
    temp_c = round(uniform(24, 25), 1)
    temp_f = temp_c * 9.0 / 5.0 + 32.0
    return temp_c, temp_f

def read_light_sensor ():
    count = randint(1, 40)
    return count

def consume_led_command():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(topics=('ledcommand'))
    for msg in consumer:
        valor = msg.value
        led = next(disp for disp in dispositivos if disp['id'] == valor['id'])
        print ('Led command received: ', valor['estado'])
        print ('Led to blink: ', valor['nome'])
        led['estado'] = valor['estado']

trd =threading.Thread(target=consume_led_command)
trd.start()

while True:
    # Read and report temperature to the cloud-based service
    (temp_c, temp_f) = read_temp()
    print('Temperature: ', temp_c, temp_f)
    if (len(dispositivos[0]['estado']) == 0 or 
        math.fabs(temp_c - dispositivos[0]['estado'][0]['temperatura']) >= 0.1):
        dat = {
            'temperatura':temp_c,
            'data':get_datetime()
        }
        if len(dispositivos[0]['estado']) == 10:
            dispositivos[0]['estado'].pop(0)
        dispositivos[0]['estado'].append(dat)
        producer.send('temperature', dispositivos[0])

    # Read and report light lelve to the cloud-based service
    light_level = read_light_sensor()
    print('Light level: ', light_level)
    if (light_level != dispositivos[1]['estado']):
        dispositivos[1]['estado'] = light_level
        producer.send('lightlevel', dispositivos[1])
    time.sleep(1)
