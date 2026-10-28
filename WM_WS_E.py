import json
import os
import socket
import sys
import threading
import time
from decimal import Decimal, InvalidOperation

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Nombres de los topics de Kafka
TOPIC_WS_COMMANDS = "wm_ws_commands"
TOPIC_WS_EVENTS = "wm_ws_events"

# Constantes del protocolo de tramas
STX, ETX = 0x02, 0x03

# Variables globales de los argumentos de entrada.
if len(sys.argv) != 6:
    print(
        "Numero de parametros no valido. Usar: "
        "WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> "
        "<puerto_monitor> <caudal_l_min>"
    )
    sys.exit(1)

# Comprobacion de los argumentos de entrada.
ip_kafka = sys.argv[1]
ip_monitor = sys.argv[3]
try:
    port_kafka = int(sys.argv[2])
    port_monitor = int(sys.argv[4])
    caudal_l_min = Decimal(sys.argv[5])
except (ValueError, InvalidOperation) as error:
    print(f"Puerto o caudal no valido: {error}")
    sys.exit(1)

# variables globales de estado del sistema de riego
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
motivo_fin_riego = None
averia_engine = threading.Event()
monitor_socket = None
monitor_send_lock = threading.Lock()

# --- FUNCIONES DEL PROTOCOLO DE TRAMAS ---

def lrc(data) -> int:
    r = 0
    for b in data:
        r^=b
    return r

def empaquetar_trama(mensaje: str) -> bytes:
    datos = mensaje.encode('utf-8')
    return bytes([STX]) + datos + bytes([ETX], lrc(datos))


def leer_byte(sock: socket.socket) -> int:
    b = sock.recv(1)
    if not b:
        raise ConnectionError("Conexion cerrada")
    return b[0]

def recibir_trama(sock: socket.socket) -> str:
    # 1. Esperar STX
    while leer_byte(sock) != STX:
        pass
            
    # 2. Leer datos hasta ETX
    datos = bytearray()
    while True:
        b = leer_byte(sock)
        if b == ETX:
            break
        datos.append(b)
        
    # 3. Leer LRC
    lrc_recibido = leer_byte(sock)
        
    # 4. Validar LRC
    if lrc(datos) != lrc_recibido:
        raise ValueError("Error LRC. Trama corrupta.")
    return datos.decode('utf-8')

# --- FUNCIONES DE PERSISTENCIA DE ESTADO ---

def obtener_nombre_archivo() -> str:
    return f"estado_riego_{ID}.json"

def guardar_estado_riego(operario: str, duracion_actual: float) -> None:
    archivo = obtener_nombre_archivo()
    estado = {
        "id_operario": operario,
        "duracion_s": duracion_actual
    }
    try:
        with open(archivo, "w") as f:
            json.dump(estado, f)
    except OSError as e:
        print(f"No se pudo guardar el estado: {e}")

def borrar_estado_riego() -> None:
    archivo = obtener_nombre_archivo()
    if os.path.exists(archivo):
        try:
            os.remove(archivo)
        except OSError:
            pass

def recuperar_estado_caida() -> None:
    archivo = obtener_nombre_archivo()
    if os.path.exists(archivo):
        print(f"Detectado cierre abrupto anterior para {ID}. Recuperando estado...")
        try:
            with open(archivo, "r") as f:
                estado = json.load(f)
            
            enviar_resumen_riego(
                operario=estado.get("id_operario", "desconocido"),
                duracion_segundos=estado.get("duracion_s", 0),
                motivo_fin="caida_engine"
            )
            print("Estado recuperado y notificado a la Central.")
        except (json.JSONDecodeError, OSError) as e:
            print(f"No se pudo recuperar el archivo de estado: {e}")
        finally:
            borrar_estado_riego()

# --- LOGICA PRINCIPAL DEL ENGINE ---

def detener_riego(motivo: str = "parada") -> None:
    global irrigacion_thread, motivo_fin_riego

    if isinstance(irrigacion_thread, threading.Thread) and irrigacion_thread.is_alive():
        motivo_fin_riego = motivo
    irrigacion_stop.set()
    if isinstance(irrigacion_thread, threading.Thread):
        irrigacion_thread.join()
        irrigacion_thread = ""


def simular_averia() -> None:
    if averia_engine.is_set():
        return

    averia_engine.set()
    with monitor_send_lock:
        if monitor_socket is not None:
            try:
                # Enviamos KO proactivamente para acelerar el cierre antes de morir
                monitor_socket.sendall(empaquetar_trama("KO"))
                print("Averia simulada: mensaje KO enviado al monitor.")
            except OSError as error:
                print(f"No se pudo enviar KO al monitor: {error}")
        else:
            print("Averia simulada, pero no hay conexion con el monitor.")
    detener_riego("averia")


def menu_operario() -> None:
    print("\n--- MENU DE CONTROL LOCAL ---")
    print("  'averia'      -> Simular averia del Engine")
    print("  'recuperar'   -> Recuperar el Engine tras una averia")
    print("  'on'          -> Habilitar el sistema")
    print("  'off'         -> Deshabilitar el sistema")
    print("  'start'       -> Iniciar riego sin limite")
    print("  'start <seg>' -> Iniciar riego con limite (ej. 'start 10')")
    print("  'stop'        -> Detener el riego")
    print("-----------------------------\n")
    
    while True:
        try:
            opcion = input("Engine> ").strip().lower()
        except EOFError:
            print("Entrada de consola no disponible.")
            return

        if not opcion:
            continue

        partes = opcion.split()
        comando = partes[0]

        if comando == "averia":
            simular_averia()
        elif comando == "recuperar":
            if averia_engine.is_set():
                averia_engine.clear()
                print("Engine recuperado; esperando confirmacion del monitor.")
            else:
                print("El Engine no esta averiado.")
        elif averia_engine.is_set():
            print("El Engine esta averiado. Usa 'recuperar' para restablecerlo.")
        elif comando in ["on", "off", "stop"]:
            mensaje = {"instruccion": comando.upper(), "id_operario": "operario_local"}
            procesar_instruccion_kafka(mensaje)
        elif comando == "start":
            mensaje = {"instruccion": "START", "id_operario": "operario_local"}
            if len(partes) > 1:
                try:
                    tiempo_val = int(partes[1])
                    mensaje["tiempo"] = tiempo_val
                except ValueError:
                    print("Error: El tiempo de riego debe ser un numero entero.")
                    continue
            procesar_instruccion_kafka(mensaje)
        else:
            print(
                "Comando no reconocido. Opciones: averia, recuperar, on, off, "
                "start [seg], stop."
            )


def responder_heartbeat() -> None:
    global ID, monitor_socket

    while True:
        if monitor_socket is None:
            nuevo_socket = None
            try:
                nuevo_socket = socket.create_connection(
                    (ip_monitor, port_monitor), timeout=5
                )
                nuevo_id = recibir_trama(nuevo_socket)
                
                if not nuevo_id or nuevo_id == "ID no valida":
                    raise ConnectionError(
                        "El monitor cerro la conexion sin enviar un ID valido."
                    )
                nuevo_socket.settimeout(None)
                monitor_socket = nuevo_socket
                ID = nuevo_id
                print(f"Conexion con el monitor restablecida. ID recibido: {ID}")
            except (OSError, ValueError) as error:
                print(
                    f"Enlace con el monitor caido: {error}. "
                    "Nuevo intento en 5 segundos."
                )
                if nuevo_socket is not None:
                    nuevo_socket.close()
                time.sleep(5)
                continue

        try:
            # Nos quedamos escuchando peticiones del monitor
            peticion = recibir_trama(monitor_socket)
            if peticion == "HB":
                with monitor_send_lock:
                    estado_a_enviar = "KO" if averia_engine.is_set() else "OK"
                    monitor_socket.sendall(empaquetar_trama(estado_a_enviar))
        except (ConnectionError, ValueError, OSError) as error:
            if monitor_socket is not None:
                monitor_socket.close()
                monitor_socket = None
            print("Enlace con el monitor perdido. Nuevo intento en 5 segundos.")
            time.sleep(5)


def enviar_telemetria(operario: str, segundo: int) -> None:
    global motivo_fin_riego

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
        motivo_fin_riego = "error_telemetria"
        irrigacion_stop.set()


def enviar_resumen_riego(
    operario: str, duracion_segundos: float, motivo_fin: str
) -> None:
    if ID == "" or not isinstance(productor_kafka, KafkaProducer):
        raise RuntimeError("Falta el ID del engine o el productor Kafka.")

    duracion = round(duracion_segundos, 3)
    volumen = (
        caudal_l_min * Decimal(str(duracion_segundos)) / Decimal(60)
    ).quantize(Decimal("0.000001"))
    resumen = {
        "tipo": "resumen_riego",
        "id": ID,
        "id_operario": operario,
        "caudal_l_min": float(caudal_l_min),
        "duration_s": duracion,
        "volume_l": float(volumen),
        "end_reason": motivo_fin,
    }
    try:
        productor_kafka.send(
            TELE_TOP,
            value=json.dumps(
                resumen, ensure_ascii=False, allow_nan=False
            ).encode("utf-8"),
        ).get(timeout=5)
        print(f"Resumen de riego enviado a la central: {resumen}")
    except (KafkaError, TimeoutError, ValueError) as error:
        print(f"Error enviando resumen de riego a la central: {error}")


def bucle_riego(operario: str, duracion: int) -> None:
    inicio = time.monotonic()
    segundo = 0
    motivo_fin = "temporizador"

    try:
        while duracion == -1 or segundo < duracion:
            siguiente_segundo = segundo + 1
            siguiente_envio = inicio + siguiente_segundo
            espera = max(0.0, siguiente_envio - time.monotonic())
            
            duracion_actual = time.monotonic() - inicio
            guardar_estado_riego(operario, duracion_actual)

            if irrigacion_stop.wait(espera):
                motivo_fin = motivo_fin_riego or "parada"
                break
            segundo = siguiente_segundo
            enviar_telemetria(operario, segundo)
            if irrigacion_stop.is_set():
                motivo_fin = motivo_fin_riego or "parada"
                break
    finally:
        duracion_real = time.monotonic() - inicio
        enviar_resumen_riego(operario, duracion_real, motivo_fin)
        borrar_estado_riego()
        print("Riego detenido.")


def procesar_instruccion_kafka(mensaje: object) -> None:
    global mensaje_kafka, instruccion_kafka, id_operario, tiempo
    global sistema_habilitado, irrigacion_thread, motivo_fin_riego

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

    if instruccion == "START" and not sistema_habilitado:
        print("No se puede iniciar el riego: el sistema no esta habilitado.")
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
            detener_riego("sistema_deshabilitado")
            print(f"Sistema deshabilitado por el operario {id_operario}.")
        case "STOP":
            detener_riego("parada")
            print(f"Riego detenido por el operario {id_operario}.")
        case "START":
            detener_riego("reemplazado")
            motivo_fin_riego = None
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
    global ID, monitor_socket

    while True:
        try:
            monitor_socket = socket.create_connection(
                (ip_monitor, port_monitor), timeout=5
            )
            
            ID = recibir_trama(monitor_socket)
            
            if not ID or ID == "ID no valida":
                raise ConnectionError(
                    "El monitor cerro la conexion sin enviar un ID valido."
                )
            monitor_socket.settimeout(None)
            print(f"Conectado al monitor {ip_monitor}:{port_monitor}.")
            print(f"ID recibido: {ID}")
            break
        except (OSError, ValueError) as error:
            print(
                f"Enlace con el monitor caido: {error}. "
                "Nuevo intento en 5 segundos."
            )
            if monitor_socket is not None:
                monitor_socket.close()
                monitor_socket = None
            time.sleep(5)

    threading.Thread(target=responder_heartbeat, daemon=True).start()
    threading.Thread(target=menu_operario, daemon=True).start()


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

    # Intentar recuperar el estado de una caida abrupta anterior
    recuperar_estado_caida()

    print(f"Escuchando ordenes en Kafka topic {COMM_TOP}.")
    try:
        while True:
            registros = consumidor.poll(timeout_ms=1000)
            if averia_engine.is_set():
                continue
            for particion_registros in registros.values():
                for registro in particion_registros:
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
        if monitor_socket is not None:
            monitor_socket.close()


escuchar_monitor()
escuchar_kafka()