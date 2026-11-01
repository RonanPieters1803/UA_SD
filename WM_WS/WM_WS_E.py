import json, os, socket, sys, threading, time
from decimal import Decimal, InvalidOperation
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Nombres de los topics de Kafka compatibles con Central
TOPIC_WS_COMMANDS = "wm_ws_commands"
TOPIC_WS_EVENTS = "wm_ws_events"

# Constantes del protocolo de tramas
STX, ETX = 0x02, 0x03

if len(sys.argv) != 6:
    print("Usar: WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor> <caudal_l_min>")
    sys.exit(1)

ip_kafka = sys.argv[1]
ip_monitor = sys.argv[3]
try:
    port_kafka = int(sys.argv[2])
    port_monitor = int(sys.argv[4])
    caudal_l_min = Decimal(sys.argv[5])
except (ValueError, InvalidOperation) as error:
    print(f"Puerto o caudal no valido: {error}")
    sys.exit(1)

ID = ""
id_operario = ""
current_request_id = None
tiempo = -1
sistema_habilitado = True
productor_kafka = None
irrigacion_stop = threading.Event()
irrigacion_stop.set()
irrigacion_thread = None
motivo_fin_riego = None
averia_engine = threading.Event()
monitor_socket = None
monitor_send_lock = threading.Lock()

# --- FUNCIONES TRAMAS ---
def lrc(data) -> int:
    r = 0
    for b in data: r^=b
    return r

#Transforma un mensaje en bytes con el formato de trama: STX + datos + ETX + LRC
def empaquetar_trama(mensaje: str) -> bytes:
  datos = mensaje.encode('utf-8')
  return bytes([STX]) + datos + bytes([ETX, lrc(datos)])

#Lee un unico byte del socket y lo devuelve como entero.
def leer_byte(sock: socket.socket) -> int:
    b = sock.recv(1)
    if not b: raise ConnectionError("Conexion cerrada")
    return b[0]

#Lee el socket de forma continua hasta encontrar STX (inicio de trama), lee el mensaje hasta ETX y se valida la integridad con LRC y devuelve el mensaje decodificado en UTF-8. 
def recibir_trama(sock: socket.socket) -> str:
    while leer_byte(sock) != STX: pass
    datos = bytearray()
    while True:
        b = leer_byte(sock)
        if b == ETX: break
        datos.append(b)
    if lrc(datos) != leer_byte(sock):
        raise ValueError("Error LRC. Trama corrupta.")
    return datos.decode('utf-8')

# --- LOGICA DEL ENGINE ---
#Señal lanzada dentro de engine o por los otros modulos para detener el riego, esperando a que se cierre el hilo de riego antes de continuar. El string pasado como parametro es el motivo de parada, por defecto "parada" indicando fin de riego manual.
def detener_riego(motivo: str = "parada") -> None:
    global irrigacion_thread, motivo_fin_riego
    if isinstance(irrigacion_thread, threading.Thread) and irrigacion_thread.is_alive():
        motivo_fin_riego = motivo
    irrigacion_stop.set()
    if isinstance(irrigacion_thread, threading.Thread):
        irrigacion_thread.join()
        irrigacion_thread = None

# Simula una averia en el engine, bloqueando el riego y enviando un mensaje de error al monitor.
def simular_averia() -> None:
    if averia_engine.is_set(): return
    averia_engine.set()
    with monitor_send_lock:
        if monitor_socket is not None:
            try:
                monitor_socket.sendall(empaquetar_trama("KO"))
            except OSError: pass
    detener_riego("averia")

# Menu de comandos para el operario, ejecutado en un hilo aparte. Permite simular averias, recuperar el engine, encender/apagar el sistema y arrancar/detener riegos manualmente.
def menu_operario() -> None:
    while True:
        try:
            print("\nComandos: averia | recuperar | on | off | stop | start [duracion_segundos]")
            opcion = input("Engine> ").strip().lower()
        except EOFError: return
        if not opcion: continue
        partes = opcion.split()
        comando = partes[0]

        if comando == "averia": simular_averia()
        elif comando == "recuperar": averia_engine.clear()
        elif comando in ["on", "off", "stop"]:
            procesar_instruccion_kafka({"instruccion": comando.upper(), "id_operario": "MANUAL"})
        elif comando == "start":
            msg = {"instruccion": "START", "id_operario": "MANUAL"}
            if len(partes) > 1:
                try: msg["tiempo"] = int(partes[1])
                except ValueError: continue
            procesar_instruccion_kafka(msg)

#Función que escucha en bucle continuo las señales de vida que envia el monitor local. Responde con un OK si funciona, KO si se averia. Si pierde conexión con el monitor, intenta reconectarse cada 5 segundos.
def responder_heartbeat() -> None:
    global ID, monitor_socket
    while True:
        if monitor_socket is None:
            try:
                ns = socket.create_connection((ip_monitor, port_monitor), timeout=5)
                nid = recibir_trama(ns)
                if not nid or nid == "ID no valida": raise ConnectionError()
                ns.settimeout(None)
                monitor_socket = ns
                ID = nid
            except (OSError, ValueError):
                time.sleep(5)
                continue
        try:
            peticion = recibir_trama(monitor_socket)
            if peticion == "HB":
                with monitor_send_lock:
                    estado = "KO" if averia_engine.is_set() else "OK"
                    monitor_socket.sendall(empaquetar_trama(estado))
        except (ConnectionError, ValueError, OSError):
            if monitor_socket is not None: monitor_socket.close()
            monitor_socket = None
            time.sleep(5)

#Funcion que envia los datos especificados de riego hacia central por kafka con el TOPIC_WS_EVENTS.
def enviar_telemetria(operario: str, segundo: int) -> None:
    volumen_acumulado = float((caudal_l_min * Decimal(segundo) / Decimal(60)).quantize(Decimal("0.000001")))
    mensaje = {
        "ws_id": ID,
        "event": "FLOW",
        "request_id": current_request_id,
        "flow": float(caudal_l_min),
        "volume": volumen_acumulado,
        "ts": time.time()
    }
    try: productor_kafka.send(TOPIC_WS_EVENTS, value=json.dumps(mensaje).encode("utf-8"))
    except: pass

#Funcion que envia un resumen final del riego hacia central por kafka con el TOPIC_WS_EVENTS, incluyendo la duracion total y el volumen total de agua regada.
def enviar_resumen_riego(operario: str, duracion_segundos: float, motivo_fin: str) -> None:
    duracion = round(duracion_segundos, 3)
    volumen = float((caudal_l_min * Decimal(str(duracion_segundos)) / Decimal(60)).quantize(Decimal("0.000001")))
    resumen = {
        "ws_id": ID,
        "event": "FINISHED",
        "request_id": current_request_id,
        "reason": motivo_fin,
        "duration": duracion,
        "volume": volumen,
        "ts": time.time()
    }
    try: productor_kafka.send(TOPIC_WS_EVENTS, value=json.dumps(resumen).encode("utf-8"))
    except: pass

#Funcion que ejecuta el riego en un bucle, usando la funcion de enviar_telemetria para enviar información cada segundo hasta que se cumpla la duracion especificada o se reciba una señal de parada. Al finalizar, envia un resumen del riego.
def bucle_riego(operario: str, duracion: int) -> None:
    inicio = time.monotonic()
    segundo = 0
    motivo_fin = "TIME"
    try:
        while duracion == -1 or segundo < duracion:
            siguiente_segundo = segundo + 1
            siguiente_envio = inicio + siguiente_segundo
            espera = max(0.0, siguiente_envio - time.monotonic())
            
            if irrigacion_stop.wait(espera):
                motivo_fin = motivo_fin_riego or "MANUAL"
                break
            segundo = siguiente_segundo
            enviar_telemetria(operario, segundo)
            if irrigacion_stop.is_set():
                motivo_fin = motivo_fin_riego or "MANUAL"
                break
    finally:
        enviar_resumen_riego(operario, time.monotonic() - inicio, motivo_fin)

#Funcion que procesa el mensaje como diccionario recibido desde kafka y procesa la instruccion de riego. 
def procesar_instruccion_kafka(mensaje: dict) -> None:
    global id_operario, tiempo, sistema_habilitado, irrigacion_thread, motivo_fin_riego, current_request_id
    
    instruccion = mensaje.get("instruccion") or mensaje.get("command")
    operario = mensaje.get("id_operario") or mensaje.get("operator_id") or "MANUAL"
    duracion = mensaje.get("tiempo") or mensaje.get("duration", -1)
    req_id = mensaje.get("request_id")
    
    if not instruccion: return
    instruccion = instruccion.strip().upper()

    if instruccion == "START" and not sistema_habilitado:
        denial = {"ws_id": ID, "event": "DENIED", "request_id": req_id, "message": "Sistema OFF"}
        productor_kafka.send(TOPIC_WS_EVENTS, value=json.dumps(denial).encode('utf-8'))
        return

    match instruccion:
        case "ON" | "ACTIVATE":
            sistema_habilitado = True
        case "OFF" | "BLOCK":
            sistema_habilitado = False
            detener_riego("BLOQUEO")
        case "STOP":
            detener_riego(mensaje.get("reason", "MANUAL"))
        case "START":
            detener_riego("REEMPLAZADO")
            motivo_fin_riego = None
            irrigacion_stop.clear()
            current_request_id = req_id
            id_operario = operario
            tiempo = duracion
            
            started_msg = {
                "ws_id": ID,
                "event": "STARTED",
                "request_id": current_request_id,
                "operator_id": id_operario,
                "duration": tiempo,
                "ts": time.time()
            }
            productor_kafka.send(TOPIC_WS_EVENTS, value=json.dumps(started_msg).encode('utf-8'))
            
            irrigacion_thread = threading.Thread(target=bucle_riego, args=(id_operario, tiempo), daemon=True)
            irrigacion_thread.start()

# Función que escucha en bucle continuo la conexión con el monitor local para recibir el ID del WS y responder a los heartbeats. Si pierde conexión, intenta reconectarse cada 5 segundos. Una vez conectado, lanza hilos para responder a heartbeats y para el menú de operario.
def escuchar_monitor() -> None:
    global ID, monitor_socket
    while True:
        try:
            monitor_socket = socket.create_connection((ip_monitor, port_monitor), timeout=5)
            ID = recibir_trama(monitor_socket)
            if not ID or ID == "ID no valida": raise ConnectionError()
            monitor_socket.settimeout(None)
            break
        except (OSError, ValueError):
            if monitor_socket is not None: monitor_socket.close()
            time.sleep(5)
    threading.Thread(target=responder_heartbeat, daemon=True).start()
    threading.Thread(target=menu_operario, daemon=True).start()

# Función que escucha en bucle continuo los mensajes de Kafka en el topic TOPIC_WS_COMMANDS y procesa las instrucciones recibidas. Si se recibe un mensaje con ws_id que coincide con el ID del WS, "ALL" o None, se procesa la instrucción. Si ocurre una avería en el engine, se ignoran los mensajes hasta que se recupere.
def escuchar_kafka() -> None:
    global productor_kafka
    brokers = [f"{ip_kafka}:{port_kafka}"]
    productor_kafka = KafkaProducer(bootstrap_servers=brokers)
    consumidor = KafkaConsumer(TOPIC_WS_COMMANDS, bootstrap_servers=brokers, group_id=f"engine-{ID}", enable_auto_commit=True)
    try:
        for registro in consumidor:
            if averia_engine.is_set(): continue
            try:
                mensaje = json.loads(registro.value.decode("utf-8"))
                if mensaje.get("ws_id") in (ID, "ALL", None):
                    procesar_instruccion_kafka(mensaje)
            except Exception: pass
    finally:
        detener_riego()
        consumidor.close()
        productor_kafka.close()
        if monitor_socket: monitor_socket.close()

#Se inician los programas principales.
escuchar_monitor()
escuchar_kafka()
