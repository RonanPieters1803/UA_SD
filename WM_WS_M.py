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


# La central valida el ID; ante KO se cierra el monitor. Los fallos de conexion se reintentan cada 5 segundos.
def escuchar_central():
    global resultado_autenticacion

    while True:
        try:
            with socket.create_connection(
                (ip_central, port_central), timeout=5
            ) as central_socket:
                autenticacion_recibida.clear()
                resultado_autenticacion = ""
                central_socket.sendall(f"{ID}\n".encode("utf-8"))
                print(f"ID {ID} enviado a la central.")
                central_socket.settimeout(None)

                with central_socket.makefile("r", encoding="utf-8") as central:
                    for instruccion in central:
                        instruccion = instruccion.rstrip("\r\n")
                        if not autenticacion_recibida.is_set():
                            match instruccion:
                                case "OK":
                                    resultado_autenticacion = instruccion
                                    autenticacion_recibida.set()
                                    print(
                                        f"Autenticacion de la central: {resultado_autenticacion}."
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
            print(f"Conexion con la central interrumpida: {error}. Reintentando.")

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
                                conexion.sendall(f"{ID}\n".encode("utf-8"))
                                print(f"ID {ID} enviado al engine en {direccion}.")
                            case "KO":
                                conexion.sendall("ID no valida\n".encode("utf-8"))
                                print(
                                    f"ID no valida; respuesta enviada al engine en {direccion}."
                                )
                            case _:
                                print(
                                    "No se puede responder al engine: "
                                    "autenticacion sin resultado valido."
                                )
                    except OSError as error:
                        print(f"No se pudo enviar la respuesta al engine: {error}")
    except OSError as error:
        print(f"Error en el servidor para el engine: {error}")
        sys.exit(1)


threading.Thread(target=escuchar_central, daemon=True).start()
servir_engine()
if cerrar_monitor.is_set():
    sys.exit(1)
