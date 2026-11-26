import json
import os
import socket
import sys
import threading
import time
import urllib3
import requests

# Desactivar avisos por certificado autofirmado en WM_Registry (HTTPS)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if len(sys.argv) not in (6, 7):
    print("Usar: WM_WS_M.py <puerto_monitor> <ip_central> <puerto_central> <ID> <localizacion> [url_registry]")
    sys.exit(1)

port_monitor = int(sys.argv[1])
ip_central = sys.argv[2]
port_central = int(sys.argv[3])
ID = sys.argv[4]
localizacion = sys.argv[5]
url_registry = sys.argv[6] if len(sys.argv) == 7 else os.environ.get("REGISTRY_URL", "https://localhost:8443")

FICHERO_CREDENCIALES = f"credenciales_{ID}.json"

# Estado de credenciales y claves
auth_token = None
symmetric_key = None
credenciales_lock = threading.Lock()

# Eventos de control
resultado_autenticacion = ""
autenticacion_recibida = threading.Event()
intentar_auth_central = threading.Event()
cerrar_monitor = threading.Event()
engine_funcionando = threading.Event()
engine_averiado = threading.Event()
engine_averiado.set()
engine_recuperado_pendiente = threading.Event()

# Eventos de actualización de clave hacia el Engine
actualizar_clave_engine = threading.Event()
revocar_clave_engine = threading.Event()

conexion_central_activa = None
envio_central_lock = threading.Lock()

# Constantes del protocolo de tramas
STX, ETX, ENQ, ACK, NACK, EOT = 0x02, 0x03, 0x05, 0x06, 0x15, 0x04


# --- PERSISTENCIA LOCAL DE CREDENCIALES DEL REGISTRY ---
def cargar_credenciales_locales():
    global auth_token
    if os.path.exists(FICHERO_CREDENCIALES):
        try:
            with open(FICHERO_CREDENCIALES, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("ws_id") == ID and data.get("auth_token"):
                    with credenciales_lock:
                        auth_token = data["auth_token"]
                    intentar_auth_central.set()
                    print(f"[INFO] Credenciales cargadas localmente desde {FICHERO_CREDENCIALES}.")
        except Exception as e:
            print(f"[WARN] No se pudieron leer las credenciales locales: {e}")


def guardar_credenciales_locales(token: str):
    global auth_token
    with credenciales_lock:
        auth_token = token
    with open(FICHERO_CREDENCIALES, "w", encoding="utf-8") as f:
        json.dump({"ws_id": ID, "location": localizacion, "auth_token": token}, f, indent=2)


def borrar_credenciales_locales():
    global auth_token, symmetric_key
    with credenciales_lock:
        auth_token = None
        symmetric_key = None
    if os.path.exists(FICHERO_CREDENCIALES):
        os.remove(FICHERO_CREDENCIALES)


# --- COMUNICACIÓN HTTPS CON WM_REGISTRY ---
def registrar_en_registry():
    print(f"[REGISTRY] Solicitando alta de {ID} ({localizacion}) en {url_registry}...")
    try:
        resp = requests.post(
            f"{url_registry}/api/ws/register",
            json={"ws_id": ID, "location": localizacion},
            verify=False,
            timeout=10,
        )
        data = resp.json()
        if resp.status_code in (200, 201) and data.get("auth_token"):
            guardar_credenciales_locales(data["auth_token"])
            print(f"[REGISTRY] Alta completada. Credencial guardada en {FICHERO_CREDENCIALES}.")
            intentar_auth_central.set()
        else:
            print(f"[REGISTRY] Error en alta ({resp.status_code}): {data.get('error', data)}")
    except Exception as e:
        print(f"[REGISTRY] Error de conexión HTTPS con WM_Registry: {e}")


def baja_en_registry():
    with credenciales_lock:
        token = auth_token
    if not token:
        print("[REGISTRY] Esta WS no tiene credenciales registradas localmente.")
        return
    try:
        resp = requests.delete(
            f"{url_registry}/api/ws/{ID}",
            headers={"Authorization": f"Bearer {token}"},
            verify=False,
            timeout=10,
        )
        if resp.status_code in (200, 204):
            borrar_credenciales_locales()
            revocar_clave_engine.set()
            print(f"[REGISTRY] Baja completada para {ID}. Credenciales eliminadas.")
        else:
            print(f"[REGISTRY] Error en baja ({resp.status_code}): {resp.text}")
    except Exception as e:
        print(f"[REGISTRY] Error de conexión HTTPS con WM_Registry: {e}")


def reautenticar_en_central():
    with credenciales_lock:
        token = auth_token
    if not token:
        print("[AUTH] No hay credenciales del Registry. Date de alta primero (opción 1).")
        return
    print("[AUTH] Solicitando nueva autenticación y clave simétrica en WM_Central...")
    intentar_auth_central.set()
    # Si ya había una conexión activa o bloqueada, forzamos reconexión limpia
    with envio_central_lock:
        if conexion_central_activa is not None:
            try:
                conexion_central_activa.close()
            except OSError:
                pass


def menu_monitor():
    while not cerrar_monitor.is_set():
        print(f"\n--- MENÚ MONITOR ({ID} - {localizacion}) ---")
        print("1. Alta en WM_Registry (HTTPS)")
        print("2. Autenticar / Re-autenticar en WM_Central")
        print("3. Baja en WM_Registry (HTTPS)")
        print("4. Ver estado actual de claves")
        try:
            opcion = input("Monitor> ").strip()
        except EOFError:
            return

        if opcion == "1":
            registrar_en_registry()
        elif opcion == "2":
            reautenticar_en_central()
        elif opcion == "3":
            baja_en_registry()
        elif opcion == "4":
            with credenciales_lock:
                t_ok = "SÍ" if auth_token else "NO"
                k_ok = "ACTIVA" if symmetric_key else "NO DISPONIBLE / REVOCADA"
            print(f"  Credencial Registry: {t_ok} | Clave Simétrica Central: {k_ok}")


# --- FUNCIONES DEL PROTOCOLO DE TRAMAS ---
def lrc(data) -> int:
    r = 0
    for b in data:
        r ^= b
    return r


def empaquetar_trama(mensaje: str) -> bytes:
    datos = mensaje.encode("utf-8")
    return bytes([STX]) + datos + bytes([ETX, lrc(datos)])


def leer_byte(sock: socket.socket) -> int:
    b = sock.recv(1)
    if not b:
        raise ConnectionError("Conexion cerrada")
    return b[0]


def recibir_trama(sock: socket.socket) -> str:
    while leer_byte(sock) != STX:
        pass
    datos = bytearray()
    while True:
        b = leer_byte(sock)
        if b == ETX:
            break
        datos.append(b)
    if lrc(datos) != leer_byte(sock):
        raise ValueError("Error LRC. Trama corrupta.")
    return datos.decode("utf-8", errors="replace")


# --- LÓGICA DE COMUNICACIÓN CON CENTRAL ---
def enviar_mensaje_central(conexion, mensaje):
    try:
        conexion.sendall(empaquetar_trama(mensaje))
        if leer_byte(conexion) == ACK:
            respuesta = recibir_trama(conexion)
            conexion.sendall(bytes([ACK]))
            return respuesta
    except Exception as e:
        print(f"[CENTRAL] Error enviando mensaje '{mensaje}': {e}")
    return None


def enviar_estado_central(conexion_central, estado):
    with envio_central_lock:
        return _enviar_estado_central_bloqueado(conexion_central, estado)


def _enviar_estado_central_bloqueado(conexion_central, estado):
    if conexion_central is None or conexion_central_activa is not conexion_central:
        return False
    resp = enviar_mensaje_central(conexion_central, estado)
    if resp == "KEY_REVOKED":
        manejar_revocacion_clave()
        return False
    return resp is not None


def manejar_revocacion_clave():
    global symmetric_key, resultado_autenticacion
    with credenciales_lock:
        symmetric_key = None
    resultado_autenticacion = "REVOKED"
    revocar_clave_engine.set()
    intentar_auth_central.clear()
    print("\n[ALERTA SEGURIDAD] WM_Central ha REVOCADO las claves de esta WS (Fuera de servicio).")
    print("[ALERTA SEGURIDAD] Usa la opción 2 del menú para volver a autenticarte.\n")


def enviar_heartbeat_central(conexion_central):
    last_ping = time.time()
    while not cerrar_monitor.wait(1):
        with envio_central_lock:
            now = time.time()
            if engine_recuperado_pendiente.is_set():
                estado_a_enviar = "LEAK_END"
            elif engine_averiado.is_set():
                estado_a_enviar = "LEAK"
            elif now - last_ping >= 15:
                estado_a_enviar = f"PING#{ID}"
                last_ping = now
            else:
                continue

            if _enviar_estado_central_bloqueado(conexion_central, estado_a_enviar):
                if estado_a_enviar == "LEAK_END":
                    engine_recuperado_pendiente.clear()
            else:
                break


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
    global resultado_autenticacion, conexion_central_activa, symmetric_key

    while not cerrar_monitor.is_set():
        if not intentar_auth_central.wait(timeout=1):
            continue

        with credenciales_lock:
            token_actual = auth_token

        if not token_actual:
            intentar_auth_central.clear()
            continue

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

                # AUTENTICACIÓN CON CREDENCIAL DEL REGISTRY
                auth_msg = f"AUTH#{ID}#{token_actual}"
                respuesta = enviar_mensaje_central(central_socket, auth_msg)

                if respuesta and respuesta.startswith("OK#"):
                    nueva_clave = respuesta.split("#", 1)[1].strip()
                    with credenciales_lock:
                        symmetric_key = nueva_clave
                    resultado_autenticacion = "OK"
                    autenticacion_recibida.set()
                    actualizar_clave_engine.set()
                    print(f"[CENTRAL] Autenticación exitosa. Clave simétrica recibida y lista para Engine.")

                    hb_thread = threading.Thread(
                        target=enviar_heartbeat_central,
                        args=(central_socket,),
                        daemon=True,
                    )
                    hb_thread.start()

                    if engine_averiado.is_set():
                        enviar_estado_central(central_socket, "LEAK")

                    hb_thread.join()
                    if resultado_autenticacion == "REVOKED":
                        continue
                    raise ConnectionError("Se perdió la conexión socket con la Central")
                else:
                    resultado_autenticacion = "KO"
                    autenticacion_recibida.set()
                    intentar_auth_central.clear()
                    print(f"[CENTRAL] Autenticación DENEGADA: {respuesta}. Verifica tu alta en WM_Registry.")

        except (OSError, UnicodeError, ConnectionError) as error:
            if resultado_autenticacion != "REVOKED":
                print(f"[CENTRAL] Enlace caído: {error}. Reintento en 5 seg.")
        finally:
            with envio_central_lock:
                if conexion_central_activa is conexion_central:
                    conexion_central_activa = None
        time.sleep(5)


# --- SERVIDOR LOCAL PARA EL ENGINE ---
def servir_engine():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor.bind(("0.0.0.0", port_monitor))
            servidor.listen()
            servidor.settimeout(1)
            print(f"[ENGINE-SRV] Escuchando en puerto {port_monitor}. Esperando autenticación para aceptar Engine...")

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
                        with credenciales_lock:
                            clave_actual = symmetric_key

                        if resultado_autenticacion == "OK" and clave_actual:
                            # Enviamos ID + clave simétrica al Engine
                            conexion.sendall(empaquetar_trama(f"INIT#{ID}#{clave_actual}"))
                            actualizar_clave_engine.clear()
                            print(f"[ENGINE-SRV] INIT (ID + Clave Simétrica) enviado a Engine en {direccion}.")
                            conexion.settimeout(1.5)

                            while not cerrar_monitor.is_set():
                                if revocar_clave_engine.is_set():
                                    conexion.sendall(empaquetar_trama("REVOKE"))
                                    revocar_clave_engine.clear()
                                    print("[ENGINE-SRV] Notificada revocación de clave al Engine.")

                                elif actualizar_clave_engine.is_set():
                                    with credenciales_lock:
                                        nueva_k = symmetric_key
                                    if nueva_k:
                                        conexion.sendall(empaquetar_trama(f"KEY#{nueva_k}"))
                                        actualizar_clave_engine.clear()
                                        print("[ENGINE-SRV] Nueva clave simétrica enviada en caliente al Engine.")

                                try:
                                    conexion.sendall(empaquetar_trama("HB"))
                                    estado_engine = recibir_trama(conexion)
                                except socket.timeout:
                                    print(f"[ENGINE-SRV] Engine {direccion} sin respuesta (averiado).")
                                    declarar_engine_averiado()
                                    break
                                except (ConnectionError, ValueError, OSError) as error:
                                    print(f"[ENGINE-SRV] Error comunicación con engine: {error}")
                                    declarar_engine_averiado()
                                    break

                                if estado_engine == "OK":
                                    engine_funcionando.set()
                                    if engine_averiado.is_set():
                                        print(f"[ENGINE-SRV] Engine {ID} restablecido.")
                                        declarar_engine_recuperado(conexion_central_activa)
                                elif estado_engine == "KO":
                                    print(f"[ENGINE-SRV] Engine {ID} averiado (KO).")
                                    declarar_engine_averiado()
                                    break

                                time.sleep(1)
                        else:
                            conexion.sendall(empaquetar_trama("ID no valida"))
                    except OSError as error:
                        print(f"[ENGINE-SRV] Error en conexión con engine: {error}")
                        declarar_engine_averiado()
                    finally:
                        engine_funcionando.clear()
    except OSError as error:
        print(f"[ENGINE-SRV] Error crítico en servidor: {error}")
        sys.exit(1)


cargar_credenciales_locales()
threading.Thread(target=menu_monitor, daemon=True).start()
threading.Thread(target=escuchar_central, daemon=True).start()
servir_engine()
if cerrar_monitor.is_set():
    sys.exit(1)