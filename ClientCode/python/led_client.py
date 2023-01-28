import logging
import sys

import grpc
import iot_service_pb2
import iot_service_pb2_grpc

from const import *


def run():
    with grpc.insecure_channel(GRPC_SERVER+':'+GRPC_PORT) as channel:
        stub = iot_service_pb2_grpc.IoTServiceStub (channel)
        username = input('User: ')
        password = input('Password: ')
        response = stub.Login(iot_service_pb2.LoginRequest(username=username, password=password))
        if response == 'OK':
            ledcolor = input('Enter the color: ')
            state = input('Enter the state: ')
            if ledcolor == 'Red' or ledcolor == 'red':
                ledId = 'led1'
            elif ledcolor == 'Green' or ledcolor == 'green':
                ledId = 'led2'
            response = stub.BlinkLed(iot_service_pb2.LedRequest(state=int(state),ledId=ledId))
            if response.ledstate == 1:
                print("Led state is on")
            else:
                print("Led state is off")
        else:
            print('Incorrect username or password')

if __name__ == '__main__':
    logging.basicConfig()
    run()
