import logging

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

from const import *


def run():
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub(channel)
        username = input('User: ')
        password = input('Password: ')
        response = stub.Login(iot_service_pb2.LoginRequest(username=username, password=password))
        if response != None:
            response = stub.SayTemperature(iot_service_pb2.TemperatureRequest(sensorId='tem1'))
            print("Temperature received: " + response.temperature)
        else:
            print('Incorrect username or password')

if __name__ == '__main__':
    logging.basicConfig()
    run()
