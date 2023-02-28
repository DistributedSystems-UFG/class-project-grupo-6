import glob
import time
import json
from kafka import KafkaProducer, KafkaConsumer
import math
import threading
import RPi.GPIO as GPIO # Import Raspberry Pi GPIO library
from const import *
from datetime import datetime

# For access to the temperature sensor
base_dir = '/sys/bus/w1/devices/'
device_folder = glob.glob(base_dir + '28*')[0]
device_file = device_folder + '/w1_slave'

# Lista dos dispositivos
dispositivos = [
    {
        'id':'tem1',
        'nome':'sensor_temperatura',
        'porta_fisica':None,
        'estado':[]
    },
    {
        'id':'lum1',
        'nome':'sensor_luminosidade',
        'porta_fisica':29,
        'estado':[]
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

# Initialize GPIO for the LEDs
GPIO.setwarnings(False) # Ignore warning for now
GPIO.setmode(GPIO.BOARD) # Use physical pin numbering
GPIO.setup(dispositivos[2]['porta_fisica'], GPIO.OUT, initial=GPIO.LOW) # Set pin 16 to be an output pin and set initial value to low (off)
GPIO.setup(dispositivos[3]['porta_fisica'], GPIO.OUT, initial=GPIO.LOW) # Idem for pin 18

# Conexao com o kafka como produtor, enviando JSON em utf-8
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Funcao para pegar a data e o horario de agora
def get_datetime():
    now = datetime.now()
    iso_date = now.isoformat()
    return str(iso_date)

# Funcao para ler a temperatura
def read_temp_raw():
    f = open(device_file, 'r')
    lines = f.readlines()
    f.close()
    return lines

# Funcao para pegar a temperatura
def read_temp():
    lines = read_temp_raw()
    while lines[0].strip()[-3:] != 'YES':
        time.sleep(0.2)
        lines = read_temp_raw()
    equals_pos = lines[1].find('t=')
    if equals_pos != -1:
        temp_string = lines[1][equals_pos+2:]
        temp_c = float(temp_string) / 1000.0
        temp_f = temp_c * 9.0 / 5.0 + 32.0
        return temp_c, temp_f

# Funcao para ler o nivel de luz
def read_light_sensor (pin_to_circuit):
    count = 0
  
    #Output on the pin for 
    GPIO.setup(pin_to_circuit, GPIO.OUT)
    GPIO.output(pin_to_circuit, GPIO.LOW)
    time.sleep(0.1)

    #Change the pin back to input
    GPIO.setup(pin_to_circuit, GPIO.IN)
  
    #Count until the pin goes high
    while (GPIO.input(pin_to_circuit) == GPIO.LOW):
        count += 1

    return count

# Funcao para mudar a cor de um led
def consume_led_command():
    #Conexao com o kafka como consumidor, recebendo JSON em utf-8
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(topics=('ledcommand'))
    for msg in consumer:
        valor = msg.value
        #Seleciona o led com base no id recebido
        led = next(disp for disp in dispositivos if disp['id'] == valor['id'])
        print ('Led command received: ', valor['estado'])
        print ('Led to blink: ', valor['nome'])
        #Define o estado do led (1 = ligado, 0 = desligado) 
        #com base no valor recebido
        led['estado'] = valor['estado']
        #If para verificar qual acao tomar
        if led['estado'] == 1:
            print ('Turning led on')
            #O led e ligado
            GPIO.output(led['porta_fisica'],GPIO.HIGH)
        else:
            print ('Turning led off')
            #O led e desligado
            GPIO.output(led['porta_fisica'],GPIO.LOW)

trd =threading.Thread(target=consume_led_command)
trd.start()

while True:
    # Read and report temperature to the cloud-based service
    (temp_c, temp_f) = read_temp()
    print('Temperature: ', temp_c, temp_f)
    #Verifica se a lista de temperaturas do sensor de temperaturas e 0,
    #se nao for, verifica se a diferenca entre a temperatura lida e a
    #ultima temperatura adicionada e maior ou igual a 0.1
    if (len(dispositivos[0]['estado']) == 0 or
        math.fabs(temp_c - dispositivos[0]['estado'][-1]['temperatura']) >= 0.1):
        #Cria uma estrutura de dados, com a temperatura e a data de agora
        dat = {
            'temperatura':temp_c,
            'data':get_datetime()
        }
        #Verifica se a lista de temperaturas e igual a 0
        if len(dispositivos[0]['estado']) == 10:
            #O primeiro item da lista, que e a temperatura mais antiga, e removido
            dispositivos[0]['estado'].pop(0)
        #E adicionado a estrutura de dados criada a lista
        dispositivos[0]['estado'].append(dat)
        #E enviado ao kafka o sensor de temperatura
        producer.send('temperature', dispositivos[0])

    # Read and report light lelve to the cloud-based service
    light_level = read_light_sensor(dispositivos[1]['porta_fisica'])
    print('Light level: ', light_level)
    #Verifica se a lista de niveis de luz do sensor de nivel de luz e 0,
    #se nao for, verifica se o nivel de luz lido e diferente do
    #ultimo nivel de luz adicionado
    if (len(dispositivos[1]['estado']) == 0 or 
        light_level != dispositivos[1]['estado'][-1]['luminosidade']):
        #Cria uma estrutura de dados, com o nivel de luz e a data de agora
        dat = {
            'luminosidade':light_level,
            'data':get_datetime()
        }
        #Verifica se a lista de niveis de luz e igual a 0
        if len(dispositivos[1]['estado']) == 10:
            #O primeiro item da lista, que e o nivel de luz mais antigo, e removido
            dispositivos[1]['estado'].pop(0)
        #E adicionado a estrutura de dados criada a lista
        dispositivos[1]['estado'].append(dat)
        #E enviado ao kafka o sensor de luminosidade
        producer.send('lightlevel', dispositivos[1])
    time.sleep(1)
