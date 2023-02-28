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
        'dispositivos': ['tem1']
    },
    {
        'id': 'usr2',
        'username':'usuario2',
        'password':'senha2',
        'dispositivos': ['lum1', 'led2']
    },
    {
        'id': 'usr3',
        'username':'usuario3',
        'password':'senha3',
        'dispositivos': ['led1']
    },
    {
        'id': 'adm1',
        'username':'admin1',
        'password':'senha1'
    }
]

# Kafka consumer to run on a separate thread
def consume_temperature():
    #Conexao com o kafka como consumidor, recebendo JSON em utf-8
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        valor = msg.value
        #Tenta achar o sensor no gemeo digital com base no id recebido
        try:
            sensor = next(disp for disp in dispositivos if disp['id'] == valor['id'])
        except StopIteration:
            sensor = None
        #Verifica se o sensor recebido nao existe como gemeo digital
        if sensor == None:
            #Adiciona o sensor recebido como gemeo digital
            dispositivos.append(valor)
            print('Device ' + valor['id'] + ' added')
        else:
            #Atualiza o estado do sensor com o valor recebido
            sensor['estado'] = valor['estado']
            print ('Received Temperature: ', sensor['estado'][-1])
        

# Kafka consumer to run on a separate thread
def consume_light_level():
    #Conexao com o kafka como consumidor, recebendo JSON em utf-8
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        valor = msg.value
        #Seleciona o gemeo digital com base no id recebido
        try:
            sensor = next(disp for disp in dispositivos if disp['id'] == valor['id'])
        except StopIteration:
            sensor = None
        #Verifica se o sensor recebido nao existe como gemeo digital
        if sensor == None:
            #Adiciona o sensor recebido como gemeo digital
            dispositivos.append(valor)
            print('Device ' + valor['id'] + ' added')
        else:
            #Atualiza o estado do sensor com o valor recebido
            sensor['estado'] = valor['estado']
            print ('Received Light Level: ', sensor['estado'][-1])

# Funcao para enviar ao kafka um led
def produce_led_command(led):
    #Conexao com o kafka como produtor, enviando JSON em utf-8
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('ledcommand', led)
        
# Classe para definir os procedimentos do gRPC
class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    #Definicao do procedimento SayTemperature
    def SayTemperature(self, request, context):
        #Seleciona o usuario com base no id recebido
        user = next(usr for usr in users if usr['id'] == request.userId)
        #Cria uma nova lista vazia
        lista = []
        #Verifica se o usuario tem permissao para usar o dispositivo
        if (request.sensorId in user['dispositivos']):
            #Seleciona o dispositivo com base no id recebido
            sensor = next(disp for disp in dispositivos if disp['id'] == request.sensorId)
            #E feito uma iteracao para cada item na lista 'estado' do dispositvo
            for dat in sensor['estado']:
                #E criado uma variavel do tipo TemperatureJSON, definido no proto,
                #usando os valores do item
                item = iot_service_pb2.TemperatureJSON(temperature=dat['temperatura'],
                    date=dat['data'])
                #E adicionado na lista criada a variavel definida acima
                lista.append(item)
        else:
            #E criado uma variavel do tipo TemperatureJSON, definido no proto,
            #usando o valor 0 para temperature e 'NA' para date
            item = iot_service_pb2.TemperatureJSON(temperature=0,
                    date='NA')
            #E adicionado na lista criada a variavel definida acima
            lista.append(item)
        #O procedimento retorna um temperatureJSON usando a lista criada
        return iot_service_pb2.TemperatureReply(temperatureJSON=lista)
    
    #Definicao do procedimento BlinkLed
    def BlinkLed(self, request, context):
        #Seleciona o usuario com base no id recebido
        user = next(usr for usr in users if usr['id'] == request.userId)
        #Verifica se o usuario tem permissao para usar o dispositivo
        if (request.ledId in user['dispositivos']):
            print ("Blink led ", request.ledId)
            print ("...with state ", request.state)
            #Seleciona o dispositivo com base no id recebido
            led = next(disp for disp in dispositivos if disp['id'] == request.ledId)
            #Update led state of twin
            led['estado'] = request.state
            #Chama a funcao para enviar o led ao kafka
            produce_led_command(led)
            #O procedimento retorna o estado do led
            return iot_service_pb2.LedReply(ledstate=led['estado'])
        else:
            #O procedimento retorna -1
            return iot_service_pb2.LedReply(ledstate=-1)

    #Definicao do procedimento SayLightLevel
    def SayLightLevel(self, request, context):
        #Seleciona o usuario com base no id recebido
        user = next(usr for usr in users if usr['id'] == request.userId)
        #Cria uma nova lista vazia
        lista = []
        #Verifica se o usuario tem permissao para usar o dispositivo
        if (request.sensorId in user['dispositivos']):
            #Seleciona o dispositivo com base no id recebido
            sensor = next(disp for disp in dispositivos if disp['id'] == request.sensorId)
            #E feito uma iteracao para cada item na lista 'estado' do dispositvo
            for dat in sensor['estado']:
                #E criado uma variavel do tipo LightLevelJSON, definido no proto,
                #usando os valores do item
                item = iot_service_pb2.LightLevelJSON(lightlevel=dat['luminosidade'],
                    date=dat['data'])
                #E adicionado na lista criada a variavel definida acima
                lista.append(item)
        else:
            #E criado uma variavel do tipo TemperatureJSON, definido no proto,
            #usando o valor 0 para lightlevel e 'NA' para date
            item = iot_service_pb2.LightLevelJSON(lightlevel=0,
                    date='NA')
            #E adicionado na lista criada a variavel definida acima
            lista.append(item)
        #O procedimento retorna um lightLevelJSON usando a lista criada
        return iot_service_pb2.LightLevelReply(lightLevelJSON=lista)

    #Definicao do procedimento Login
    def Login(self, request, context):
        #E feito uma iteracao para cada usuario cadastrado no sistema
        for user in users:
            #Verifica se o username e password passado e o mesmo do usuario
            if user['username'] == request.username and \
                user['password'] == request.password:
                #O procedimento retorna o id do usuario
                return iot_service_pb2.LoginReply(userId=user['id'])
        #O procedimento retorna vazio
        return iot_service_pb2.LoginReply(userId=None)
    
    #Definicao do procedimento GetUserDevices
    def GetUserDevices(self, request, context):
        #Seleciona o usuario com base no id recebido
        user = next(usr for usr in users if usr['id'] == request.userId)
        #O procedimento retorna os dispositivos do usuario
        return iot_service_pb2.GetDeviceReply(deviceId = user['dispositivos'])

    #Definicao do procedimento AddUserDevice
    def AddUserDevice(self, request, context):
        #Seleciona o usuario com base no id recebido
        try:
            user = next(usr for usr in users if usr['id'] == request.userId)
        except StopIteration:
            #O procedimento retorna a string 'ERROR'
            return iot_service_pb2.DeviceReply(confirmation='ERROR')
        #Verifica se o dispositivo ja nao esta presente na lista do usuario
        if request.deviceId not in user['dispositivos']:
            #Adiciona o id do dispositivo na lista de dispositivos do usuario
            user['dispositivos'].append(request.deviceId)
        #O procedimento retorna a string 'OK'
        return iot_service_pb2.DeviceReply(confirmation='OK')

    #Definicao do procedimento RemoveUserDevice
    def RemoveUserDevice(self, request, context):
        #Seleciona o usuario com base no id recebido
        try:
            user = next(usr for usr in users if usr['id'] == request.userId)
        except StopIteration:
            #O procedimento retorna a string 'ERROR'
            return iot_service_pb2.DeviceReply(confirmation='ERROR')
        #E removido o id do dispositivo da lista de dispositivo do usuario
        try:
            user['dispositivos'].remove(request.deviceId)
        except ValueError:
            #O procedimento retorna a string 'OK'
            return iot_service_pb2.DeviceReply(confirmation='OK')
        #O procedimento retorna a string 'OK'
        return iot_service_pb2.DeviceReply(confirmation='OK')
        

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
