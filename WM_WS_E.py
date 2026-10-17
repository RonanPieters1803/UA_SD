import json
import socket
import sys
import threading
import time
from decimal import Decimal, InvalidOperation

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

#Nombres de los topics de Kafka
COMM_TOP = "riego.comandos"
TELE_TOP = "riego.telemetria"

# Variables globales de los argumentos de entrada.
if len(sys.argv) != 6:
    print(
        "Numero de parametros no valido. Usar: "
        "WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> "
        "<puerto_monitor> <caudal_l_min>"
    )
    sys.exit(1)
#Comprobacion de los argumentos de entrada.
ip_kafka = sys.argv[1]
ip_monitor = sys.argv[3]
try:
    port_kafka = int(sys.argv[2])
    port_monitor = int(sys.argv[4])
    caudal_l_min = Decimal(sys.argv[5])
except (ValueError, InvalidOperation) as error:
    print(f"Puerto o caudal no valido: {error}")
    sys.exit(1)

#variables globales de estado del sistema de riego
ID = ""
mensaje_kafka = ""
instruccion_kafka = ""
id_operario = ""
tiempo = -1
sistema_habilitado = False
productor_kafka = ""
irrigacion_stop = threading.Event()
irrigacion_stop.set()
irrigacion_thread = ""

# Funcion para detener el riego y esperar a que el hilo de riego termine.
def detener_riego() -> None:
    global irrigacion_thread

    irrigacion_stop.set()
    if isinstance(irrigacion_thread, threading.Thread):
        irrigacion_thread.join()
        irrigacion_thread = ""

#Funcion para enviarle a la central atraves de kafka la telemetria del riego.
def enviar_telemetria(operario: str, segundo: int) -> None:
    if ID == "" or not isinstance(productor_kafka, KafkaProducer):
        raise RuntimeError("Falta el ID del engine o el productor Kafka.")

    volumen_acumulado = (
        caudal_l_min * Decimal(segundo) / Decimal(60)
    ).quantize(Decimal("0.000001"))
    mensaje = {
        "id": ID,
        "caudal_l_min": float(caudal_l_min),
        "volumen_acumulado_l": float(volumen_acumulado),
        "id_operario": operario,
    }
    try:
        productor_kafka.send(
            TELE_TOP,
            value=json.dumps(
                mensaje, ensure_ascii=False, allow_nan=False
            ).encode("utf-8"),
        ).get(timeout=5)
        print(f"Telemetria enviada: {mensaje}")
    except (KafkaError, TimeoutError, ValueError) as error:
        print(f"Error enviando telemetria Kafka: {error}")
        irrigacion_stop.set()


def bucle_riego(operario: str, duracion: int) -> None:
    inicio = time.monotonic()
    segundo = 0

    while duracion == -1 or segundo < duracion:
        segundo += 1
        siguiente_envio = inicio + segundo
        espera = max(0.0, siguiente_envio - time.monotonic())
        if irrigacion_stop.wait(espera):
            break
        enviar_telemetria(operario, segundo)
        if irrigacion_stop.is_set():
            break

    print("Riego detenido.")


def procesar_instruccion_kafka(mensaje: object) -> None:
    global mensaje_kafka, instruccion_kafka, id_operario, tiempo
    global sistema_habilitado, irrigacion_thread

    if not isinstance(mensaje, dict):
        print(f"Formato de instruccion JSON no valido: {mensaje}")
        return

    instruccion = mensaje.get("instruccion")
    operario = mensaje.get("id_operario")
    if not isinstance(instruccion, str) or not isinstance(operario, str):
        print(f"Falta la instruccion o el ID del operario: {mensaje}")
        return

    instruccion = instruccion.strip()
    operario = operario.strip()
    duracion = -1

    if not operario:
        print(f"Falta el ID del operario: {mensaje}")
        return

    match instruccion:
        case "ON" | "OFF" | "STOP":
            if set(mensaje) != {"instruccion", "id_operario"}:
                print(f"Formato de instruccion no valido: {mensaje}")
                return
        case "START":
            if set(mensaje) not in (
                {"instruccion", "id_operario"},
                {"instruccion", "id_operario", "tiempo"},
            ):
                print(f"Formato de instruccion START no valido: {mensaje}")
                return
            valor_tiempo = mensaje.get("tiempo")
            if valor_tiempo is not None:
                if type(valor_tiempo) is not int or valor_tiempo < 0:
                    print(f"Tiempo de START no valido: {mensaje}")
                    return
                duracion = valor_tiempo
        case _:
            print(f"Instruccion Kafka desconocida: {instruccion}")
            return

    mensaje_kafka = json.dumps(mensaje, ensure_ascii=False)
    instruccion_kafka = instruccion
    id_operario = operario
    tiempo = duracion

    match instruccion:
        case "ON":
            sistema_habilitado = True
            print(f"Sistema habilitado por el operario {id_operario}.")
        case "OFF":
            sistema_habilitado = False
            detener_riego()
            print(f"Sistema deshabilitado por el operario {id_operario}.")
        case "STOP":
            detener_riego()
            print(f"Riego detenido por el operario {id_operario}.")
        case "START":
            detener_riego()
            irrigacion_stop.clear()
            irrigacion_thread = threading.Thread(
                target=bucle_riego,
                args=(id_operario, tiempo),
                daemon=True,
            )
            irrigacion_thread.start()
            if tiempo == -1:
                print(f"Riego iniciado por {id_operario} sin limite de tiempo.")
            else:
                print(
                    f"Riego iniciado por {id_operario} durante {tiempo} segundos."
                )


def escuchar_monitor() -> None:
    global ID

    try:
        with socket.create_connection(
            (ip_monitor, port_monitor), timeout=5
        ) as monitor_socket:
            monitor_socket.settimeout(None)
            print(f"Conectado al monitor {ip_monitor}:{port_monitor}.")

            with monitor_socket.makefile("r", encoding="utf-8") as monitor:
                for mensaje in monitor:
                    ID = mensaje.rstrip("\r\n")
                    if ID:
                        print(f"ID recibido: {ID}")
                        return

        raise ConnectionError("El monitor cerro la conexion sin enviar un ID.")
    except (OSError, UnicodeError, ValueError) as error:
        print(f"Error al recibir el ID del monitor: {error}")
        sys.exit(1)


def escuchar_kafka() -> None:
    global productor_kafka

    if ID == "":
        print("No se puede iniciar Kafka sin un ID del monitor.")
        sys.exit(1)

    brokers = [f"{ip_kafka}:{port_kafka}"]
    try:
        productor_kafka = KafkaProducer(
            bootstrap_servers=brokers,
            max_block_ms=5000,
            request_timeout_ms=10000,
        )
        consumidor = KafkaConsumer(
            COMM_TOP,
            bootstrap_servers=brokers,
            group_id=f"engine-{ID}",
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
    except (KafkaError, OSError, ValueError) as error:
        print(f"No se pudo conectar con Kafka: {error}")
        sys.exit(1)

    print(f"Escuchando ordenes en Kafka topic {COMM_TOP}.")
    try:
        for registro in consumidor:
            try:
                mensaje = json.loads(registro.value.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as error:
                print(f"Mensaje Kafka no es JSON UTF-8 valido: {error}")
                continue
            procesar_instruccion_kafka(mensaje)
    except KafkaError as error:
        print(f"Error escuchando ordenes Kafka: {error}")
        sys.exit(1)
    finally:
        detener_riego()
        consumidor.close()
        if isinstance(productor_kafka, KafkaProducer):
            productor_kafka.close(timeout=5)


escuchar_monitor()
escuchar_kafka()
