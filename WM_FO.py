import json
from socket import error
import sys
import time
import uuid
from datetime import datetime
 
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

#Nombres de los topics de Kafka
TOPIC_FO_REQUESTS = "wm_fo_requests"
TOPIC_FO_EVENTS = "wm_fo_events" 

APP_NAME = "WM_FO"
WAIT_BETWEEN_SERVICES = 4  # Tiempo de espera entre solicitudes en segundos
RESPONSE_TIMEOUT = 30  # Tiempo de espera para recibir una respuesta en segundos de central

def log(msg):
    print(f"[{APP_NAME}] {datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)

def main():
    if len(sys.argv) != 4:
        print("Numero de parametros no valido. Usar: "
        "WM_FO.py <ip_kafka> <puerto_kafka>  "
        "<id_operario>")
        sys.exit(1)

    ip_kafka = sys.argv[1]
    puerto_kafka = sys.argv[2]
    id_operario = sys.argv[3]
    services_file = sys.argv[4] if len(sys.argv) > 4 else None

    try:
        port_kafka = int(puerto_kafka)
    except ValueError as error:
        print(f"Puerto no valido: {error}")
        sys.exit(1)

    log(f"Operador '{id_operario}' conectando a Kafka en {ip_kafka}:{port_kafka}")

