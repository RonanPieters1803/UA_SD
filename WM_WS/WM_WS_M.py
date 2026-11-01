import socket
import sys
import threading
import time

if len(sys.argv) != 6:
    print("Numero de parametros no valido.")
    sys.exit(1)

port_monitor = int(sys.argv[1])
ip_central = sys.argv[2]
port_central = int(sys.argv[3])
ID = sys.argv[4]
localizacion = sys.argv[5]

resultado_autenticacion = ""
autenticacion_recibida = threading.Event()
cerrar_monitor = threading.Event()
engine_funcionando = threading.Event()
engine_averiado = threading.Event()
engine_averiado.set()
conexion_central_activa = None
envio_central_lock = threading.Lock()
engine_recuperado_pendiente = threading.Event()

# Constantes del protocolo de tramas
STX, ETX, ENQ, ACK, NACK, EOT = 0x02, 0x03, 0x05, 0x06, 0x15, 0x04

# --- FUNCIONES DEL PROTOCOLO DE TRAMAS ---
def lrc(data) -> int:
    r = 0
    for b in data:
        r ^= b
    return r

#Transforma un mensaje en bytes con el formato de trama: STX + datos + ETX + LRC
def empaquetar_trama(mensaje: str) -> bytes:
    datos = mensaje.encode('utf-8')
    return bytes([STX]) + datos + bytes([ETX, lrc(datos)])

#Lee un unico byte del socket y lo devuelve como entero.
def leer_byte(sock: socket.socket) -> int:
    b = sock.recv(1)
    if not b:
        raise ConnectionError("Conexion cerrada")
    return b[0]

#Lee el socket de forma continua hasta encontrar STX (inicio de trama), lee el mensaje hasta ETX y se valida la integridad con LRC y devuelve el mensaje decodificado en UTF-8. 
def recibir_trama(sock: socket.socket) -> str:
    while leer_byte(sock) != STX:
        pass
    datos = bytearray()
    while True:
        b = leer_byte(sock)
        if b == ETX:
            break
        datos.append(b)
    lrc_recibido = leer_byte(sock)
    if lrc(datos) != lrc_recibido:
        raise ValueError("Error LRC. Trama corrupta.")
    return datos.decode('utf-8', errors='replace')

# --- LOGICA PRINCIPAL DEL MONITOR ---
#Envía un mensaje a la Central respetando el protocolo de tramas y ACK.
def enviar_mensaje_central(conexion, mensaje):
    try:
        conexion.sendall(empaquetar_trama(mensaje))
        if leer_byte(conexion) == ACK:
            respuesta = recibir_trama(conexion)
            conexion.sendall(bytes([ACK]))
            return respuesta
    except Exception as e:
        print(f"Error enviando mensaje '{mensaje}' a Central: {e}")
    return None
#Función simple que envia un estado a la Central, bloqueando el acceso al socket para evitar colisiones de envío. Devuelve True si se envió correctamente, False si no se pudo enviar.
def enviar_estado_central(conexion_central, estado):
    with envio_central_lock:
        return _enviar_estado_central_bloqueado(conexion_central, estado)

#funcion interna que envia un estado a la Central, asumiendo que el acceso al socket ya está bloqueado. Devuelve True si se envió correctamente, False si no se pudo enviar.
def _enviar_estado_central_bloqueado(conexion_central, estado):
    if conexion_central is None or conexion_central_activa is not conexion_central:
        return False
    resp = enviar_mensaje_central(conexion_central, estado)
    return resp is not None

#funcion que envia un heartbeat a la central cada 15 segundos, o envia un estado de avería o recuperación si corresponde. Se ejecuta en un hilo separado y termina si se cierra el monitor o si se pierde la conexión con la central.
def enviar_heartbeat_central(conexion_central):
    last_ping = time.time()
    while not cerrar_monitor.wait(1):
        with envio_central_lock:
            now = time.time()
            if engine_recuperado_pendiente.is_set():
                estado_a_enviar = "LEAK_END"
            elif engine_averiado.is_set():
                estado_a_enviar = "LEAK"
            elif now - last_ping >= 15:  # Enviar PING cada 15s para no superar los 30s de la Central
                estado_a_enviar = f"PING#{ID}"
                last_ping = now
            else:
                continue
                
            if _enviar_estado_central_bloqueado(conexion_central, estado_a_enviar):
                if estado_a_enviar == "LEAK_END":
                    engine_recuperado_pendiente.clear()
            else:
                break

#funcion quem en caso de no recibir el heartbeat del engine o recibir un estado KO, declara el engine como averiado y envia un estado LEAK a la central. Se ejecuta en el hilo principal del monitor.
def declarar_engine_averiado():
    engine_funcionando.clear()
    if not engine_averiado.is_set():
        engine_averiado.set()
        enviar_estado_central(conexion_central_activa, "LEAK")

#funcion que en caso de recibir un estado OK del engine, declara el engine como recuperado y envia un estado LEAK_END a la central. Se ejecuta en el hilo principal del monitor.
def declarar_engine_recuperado(conexion_central):
    with envio_central_lock:
        if engine_averiado.is_set():
            engine_averiado.clear()
            engine_recuperado_pendiente.set()
            if _enviar_estado_central_bloqueado(conexion_central, "LEAK_END"):
                engine_recuperado_pendiente.clear()

#Funcion que escucha en bucle continuo la conexión con la central, realiza el handshake inicial, la autenticación y luego lanza un hilo para enviar heartbeat y estados. Si se pierde la conexión, intenta reconectar cada 5 segundos. Si la autenticación es rechazada, cierra el monitor.
def escuchar_central():
    global resultado_autenticacion, conexion_central_activa

    while not cerrar_monitor.is_set():
        conexion_central = None
        try:
            with socket.create_connection((ip_central, port_central), timeout=5) as central_socket:
                conexion_central = central_socket
                with envio_central_lock:
                    conexion_central_activa = central_socket
                
                # HANDSHAKE INICIAL
                central_socket.sendall(bytes([ENQ]))
                if leer_byte(central_socket) != ACK:
                    raise ConnectionError("No se recibió ACK tras ENQ")

                # AUTENTICACIÓN
                reg_msg = f"REG#{ID}#{localizacion}"
                respuesta = enviar_mensaje_central(central_socket, reg_msg)
                
                if respuesta and respuesta.startswith("OK"):
                    resultado_autenticacion = "OK"
                    autenticacion_recibida.set()
                    print(f"Autenticacion de la central: {respuesta}.")

                    hb_thread = threading.Thread(
                        target=enviar_heartbeat_central,
                        args=(central_socket,),
                        daemon=True,
                    )
                    hb_thread.start()

                    # Notificar a Central que el Engine aún no está conectado (LEAK)
                    if engine_averiado.is_set():
                        enviar_estado_central(central_socket, "LEAK")
                        
                    # Espera a que el hilo de heartbeat/envío detecte una caída de socket
                    hb_thread.join()
                    raise ConnectionError("Se perdió la conexión socket con la Central")
                else:
                    resultado_autenticacion = "KO"
                    autenticacion_recibida.set()
                    cerrar_monitor.set()
                    print(f"Autenticacion rechazada (KO): {respuesta}. Cerrando.")
                    return

        except (OSError, UnicodeError, ConnectionError) as error:
            print(f"Enlace con la central caido: {error}. Nuevo intento en 5 seg.")
        finally:
            with envio_central_lock:
                if conexion_central_activa is conexion_central:
                    conexion_central_activa = None
        time.sleep(5)

#Funcion que sirve como servidor para el engine, aceptando conexiones y enviando el ID tras la autenticación. Luego entra en un bucle de heartbeat con el engine, declarando avería si no responde o si envía KO. Se ejecuta en el hilo principal del monitor.
def servir_engine():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor.bind(("0.0.0.0", port_monitor))
            servidor.listen()
            servidor.settimeout(1)
            print(f"Servidor para el engine escuchando en el puerto {port_monitor}.")

            while not cerrar_monitor.is_set():
                if not autenticacion_recibida.wait(timeout=1):
                    continue
                if cerrar_monitor.is_set():
                    break
                try:
                    conexion, direccion = servidor.accept()
                except socket.timeout:
                    continue
                with conexion:
                    if cerrar_monitor.is_set():
                        break
                    try:
                        match resultado_autenticacion:
                            case "OK":
                                conexion.sendall(empaquetar_trama(ID))
                                print(f"ID {ID} enviado al engine en {direccion}.")
                                conexion.settimeout(1.0)
                                enlace_ko_recibido = False
                                
                                while not cerrar_monitor.is_set():
                                    try:
                                        conexion.sendall(empaquetar_trama("HB"))
                                        estado_engine = recibir_trama(conexion)
                                    except socket.timeout:
                                        print(f"Engine {direccion} sin respuesta 1s (averiado).")
                                        declarar_engine_averiado()
                                        break
                                    except (ConnectionError, ValueError, OSError) as error:
                                        print(f"Error comunicacion con engine: {error}")
                                        declarar_engine_averiado()
                                        break

                                    match estado_engine:
                                        case "OK":
                                            engine_funcionando.set()
                                            if engine_averiado.is_set():
                                                print(f"Engine {ID} restablecido.")
                                                declarar_engine_recuperado(conexion_central_activa)
                                        case "KO":
                                            print(f"Engine {ID} averiado (KO).")
                                            declarar_engine_averiado()
                                            enlace_ko_recibido = True
                                            break
                                    if enlace_ko_recibido:
                                        break
                                    time.sleep(1)
                                    
                            case "KO":
                                conexion.sendall(empaquetar_trama("ID no valida"))
                    except OSError as error:
                        print(f"Error en conexion con el engine: {error}")
                        declarar_engine_averiado()
                    finally:
                        engine_funcionando.clear()
    except OSError as error:
        print(f"Error en servidor para el engine: {error}")
        sys.exit(1)

#Se inicia un hilo para escuchar a la central y se ejecuta la función principal de servir al engine. Si se cierra el monitor, se termina el programa.
threading.Thread(target=escuchar_central, daemon=True).start()
servir_engine()
if cerrar_monitor.is_set():
    sys.exit(1)