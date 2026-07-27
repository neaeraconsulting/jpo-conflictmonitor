import csv
import socket
import os

CSV_FILE = 'SSM-SRM-Example-1.csv'
DOCKER_HOST_IP = os.getenv('DOCKER_HOST_IP')
UDP_PORT = 44990

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

with open(f'{CSV_FILE}', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        asn1_hex = row.get('asn1')
        if asn1_hex:
            data = bytes.fromhex(asn1_hex)
            sock.sendto(data, (DOCKER_HOST_IP, UDP_PORT))
            print(f'Sent {len(data)} bytes to {DOCKER_HOST_IP}:{UDP_PORT}')
