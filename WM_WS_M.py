import socket
import sys
import threading
import time

if len(sys.argv) != 6:
    print("Numero de parametros no valido.")
    sys.exit(1)

# Variables globales de los argumentos de entrada.
port_monitor = int(sys.argv[1])
ip_central = sys.argv[2]
port_central = int(sys.argv[3])
ID = sys.argv[4]
localizacion = sys.argv[5]
estado = "desconectado"  # Estado inicial del sistema de riego
resultado_autenticacion = ""
autenticacion_recibida = threading.Event()
cerrar_monitor = threading.Event()
engine_funcionando = threading.Event()
engine_averiado = threading.Event()
conexion_central_activa = None
envio_central_lock = threading.Lock()
engine_recuperado_pendiente = threading.Event()

# Constantes del protocolo de tramas
STX, ETX, ENQ, ACK, NACK, EOT = 0x02, 0x03, 0x05, 0x06, 0x15, 0x04

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

# --- LOGICA PRINCIPAL DEL MONITOR ---

def enviar_estado_central(conexion_central, estado):
    with envio_central_lock:
        return _enviar_estado_central_bloqueado(conexion_central, estado)


def _enviar_estado_central_bloqueado(conexion_central, estado):
    if (
        conexion_central is None
        or conexion_central_activa is not conexion_central
    ):
        return False
    if estado == "OK" and not engine_funcionando.is_set():
        return True

    try:
        conexion_central.sendall(f"{estado}\n".encode("utf-8"))
        print(f"Estado {estado} del engine enviado a la central.")
        return True
    except OSError as error:
        print(f"No se pudo enviar el estado {estado} a la central: {error}")
        return False


def enviar_heartbeat_central(conexion_central):
    while not cerrar_monitor.wait(1):
        with envio_central_lock:
            if engine_recuperado_pendiente.is_set():
                estado_a_enviar = "LEAK_END"
            else:
                estado_a_enviar = "LEAK" if engine_averiado.is_set() else "OK"
            if not _enviar_estado_central_bloqueado(
                conexion_central, estado_a_enviar
            ):
                break
            if estado_a_enviar == "LEAK_END":
                engine_recuperado_pendiente.clear()


def declarar_engine_averiado():
    engine_funcionando.clear()
    if not engine_averiado.is_set():
        engine_averiado.set()
        enviar_estado_central(conexion_central_activa, "LEAK")


def declarar_engine_recuperado(conexion_central):
    with envio_central_lock:
        if engine_averiado.is_set():
            engine_averiado.clear()
            engine_recuperado_pendiente.set()
            if _enviar_estado_central_bloqueado(conexion_central, "LEAK_END"):
                engine_recuperado_pendiente.clear()


def escuchar_central():
    global resultado_autenticacion, conexion_central_activa

    while True:
        conexion_central = None
        try:
            with socket.create_connection(
                (ip_central, port_central), timeout=5
            ) as central_socket:
                conexion_central = central_socket
                with envio_central_lock:
                    conexion_central_activa = central_socket
                autenticacion_local = False
                
                central_socket.sendall(f"{ID}\n".encode("utf-8"))
                print(f"ID {ID} enviado a la central.")
                central_socket.settimeout(None)

                with central_socket.makefile("r", encoding="utf-8") as central:
                    for instruccion in central:
                        instruccion = instruccion.rstrip("\r\n")
                        if not autenticacion_local:
                            match instruccion:
                                case "OK":
                                    autenticacion_local = True
                                    resultado_autenticacion = instruccion
                                    autenticacion_recibida.set()
                                    print(
                                        f"Autenticacion de la central: {resultado_autenticacion}."
                                    )
                                    threading.Thread(
                                        target=enviar_heartbeat_central,
                                        args=(central_socket,),
                                        daemon=True,
                                    ).start()
                                    if engine_averiado.is_set():
                                        enviar_estado_central(
                                            central_socket, "LEAK"
                                        )
                                case "KO":
                                    resultado_autenticacion = instruccion
                                    autenticacion_recibida.set()
                                    cerrar_monitor.set()
                                    print(
                                        "Autenticacion rechazada por la central (KO). "
                                        "Cerrando el monitor."
                                    )
                                    return
                                case _:
                                    print(
                                        f"Respuesta de autenticacion no valida: {instruccion}"
                                    )
                        else:
                            print(
                                f"Instruccion recibida de la central: {instruccion}"
                            )

                print("La central cerro la conexion; reintentando.")
        except (OSError, UnicodeError) as error:
            print(
                f"Enlace con la central caido: {error}. "
                "Nuevo intento en 5 segundos."
            )
        finally:
            with envio_central_lock:
                if conexion_central_activa is conexion_central:
                    conexion_central_activa = None

        time.sleep(5)


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
                                        # 1. El Monitor pide activamente el estado
                                        conexion.sendall(empaquetar_trama("HB"))
                                        
                                        # 2. Espera la respuesta (con timeout de 1 seg)
                                        estado_engine = recibir_trama(conexion)
                                    except socket.timeout:
                                        print(
                                            f"Engine {direccion} sin respuesta durante 1 segundo; "
                                            "se considera averiado."
                                        )
                                        declarar_engine_averiado()
                                        break
                                    except (ConnectionError, ValueError, OSError) as error:
                                        print(f"Error comunicacion con el engine: {error}")
                                        declarar_engine_averiado()
                                        break

                                    match estado_engine:
                                        case "OK":
                                            engine_funcionando.set()
                                            if engine_averiado.is_set():
                                                print(
                                                    f"Engine {ID} restablecido."
                                                )
                                                declarar_engine_recuperado(
                                                    conexion_central_activa
                                                )
                                        case "KO":
                                            print(
                                                f"Engine {ID} averiado (KO)."
                                            )
                                            declarar_engine_averiado()
                                            enlace_ko_recibido = True
                                            break
                                        case _:
                                            print(
                                                "Respuesta del engine no valida: "
                                                f"{estado_engine}"
                                            )
                                    if enlace_ko_recibido:
                                        break
                                        
                                    # Espera 1 segundo para mandar la siguiente comprobacion
                                    time.sleep(1)
                                    
                            case "KO":
                                conexion.sendall(empaquetar_trama("ID no valida"))
                                print(
                                    f"ID no valida; respuesta enviada al engine en {direccion}."
                                )
                            case _:
                                print(
                                    "No se puede responder al engine: "
                                    "autenticacion sin resultado valido."
                                )
                    except OSError as error:
                        print(f"Error en la conexion con el engine: {error}")
                        declarar_engine_averiado()
                    finally:
                        engine_funcionando.clear()
    except OSError as error:
        print(f"Error en el servidor para el engine: {error}")
        sys.exit(1)


threading.Thread(target=escuchar_central, daemon=True).start()
servir_engine()
if cerrar_monitor.is_set():
    sys.exit(1)