import argparse
import hmac
import json
import os
import socket
import sqlite3
import threading
import time
import uuid
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_NAME = "WM_Central"

#### POSIBLES TOPICS DE KAFKA
TOPIC_FO_REQUESTS = "wm_fo_requests"    # FO      -> CENTRAL
TOPIC_FO_EVENTS = "wm_fo_events"        # CENTRAL -> FO
TOPIC_WS_COMMANDS = "wm_ws_commands"    # CENTRAL -> Engine
TOPIC_WS_EVENTS = "wm_ws_events"        # Engine  -> CENTRAL
TOPIC_WS_STATUS = "wm_ws_status"        # CENTRAL -> todos (estado de las WS)
ALL_TOPICS = [TOPIC_FO_REQUESTS, TOPIC_FO_EVENTS, TOPIC_WS_COMMANDS, TOPIC_WS_EVENTS, TOPIC_WS_STATUS]

#### ESTADOS DE LAS ESTACIONES
AVAILABLE = "DISPONIBLE"
WATERING = "REGANDO"
LEAK = "FUGA"
OUT_OF_SERVICE = "FUERA_DE_SERVICIO"
OFFLINE = "DESCONECTADA"

#### DURACIONES POR DEFECTO
DEFAULT_DURATION = 30
MAX_DURATION = 3600
AUTH_TIMEOUT = 10
IDLE_TIMEOUT = 30
STATUS_PERIOD = 2

REASON_TEXT = {
    "TIME": "límite de tiempo alcanzado",
    "MANUAL": "parada manual en la estación",
    "FUGA": "fuga detectada",
    "BLOQUEO": "WS puesta fuera de servicio por CENTRAL",
    "DESCONEXION": "WS desconectada",
}

INTERNAL_OPERATORS = {"CENTRAL", "MANUAL"}

#### CARACTERES DEL SOCKET
STX, ETX, ENQ, ACK, NACK, EOT = 0x02, 0x03, 0x05, 0x06, 0x15, 0x04

def lrc(data:bytes) -> int: #LCR = XOR de todos los bytes del mensaje
    r = 0
    for b in data:
        r ^= b
    return r

def build_frame(text: str) -> bytes:
    data = text.encode("utf-8")
    return bytes([STX]) + data + bytes([ETX, lrc(data)])


#### CLASES NECESARIAS
class MonitorConnection: #Connection TCP con una WS
    def __init__(self, sock):
        self.sock = sock
        self.reader = sock.makefile("rb")
        self.ws_id = None

    def read_byte(self) -> int:
        b = self.reader.read(1)
        if not b:
            raise ConnectionError("Conexión cerrada")
        return b[0]

    def read_frame_body(self):
        data = bytearray()
        while True:
            b = self.read_byte()
            if b == ETX:
                break
            data.append(b)
        return bytes(data), self.read_byte()

    def close(self):
        for closer in (self.reader.close, self.sock.close):
            try:
                closer()
            except OSError:
                pass


class DataBase:
    def __init__(self, path): #inicializar sqlite con 3 tablas: operators, watering_stations, irrigations si aun no existen
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS operators (
                    id   TEXT PRIMARY KEY,
                    name TEXT
                );
                CREATE TABLE IF NOT EXISTS watering_stations (
                    id            TEXT PRIMARY KEY,
                    location      TEXT NOT NULL,
                    blocked       INTEGER NOT NULL DEFAULT 0,
                    registered_at TEXT
                );
                CREATE TABLE IF NOT EXISTS irrigations (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    ws_id       TEXT,
                    operator_id TEXT,
                    request_id  TEXT,
                    started_at  TEXT,
                    ended_at    TEXT,
                    duration_s  REAL,
                    volume_l    REAL,
                    end_reason  TEXT
                );
            """)
            self.conn.commit()

    def execute(self, sql, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur

    def query(self, sql, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            return cur.fetchall()

class KafkaBus:
    def __init__(self, bootstrap, log): #inicializar kafka producer con bootstrap y log, con acks=all y retries=3
        from kafka import KafkaProducer
        self.bootstrap = bootstrap
        self.log = log
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"), #Crear un productor de Kafka que convierte los mensajes a JSON y los codifica en UTF-8
            acks="all",
            retries=3,
        )

    def ensure_topics(self): #Crear los topics de Kafka si no existen, ignorando el error si ya existen
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError
            admin = KafkaAdminClient(bootstrap_servers=self.bootstrap)
            try:
                admin.create_topics([NewTopic(t, 1, 1) for t in ALL_TOPICS])
            except TopicAlreadyExistsError:
                pass
            finally:
                admin.close()
        except Exception as e: #El broker puede tener auto-creación activada, en ese caso no hace falta crear los topics
            self.log(f"Error al crear topics: {e}")

    def publish(self,topic, payload): #Publicar un mensaje de forma asíncrona en un topic de Kafka, logueando el resultado
        future = self.producer.send(topic, payload)
        future.add_errback(lambda e: self.log(f"Error al publicar mensaje en {topic}: {e}"))

    def consume_forever(self, handler): #Bucle infinito que consume mensajes de Kafka y los pasa a un handler
        from kafka import KafkaConsumer
        while True:
            try:
                consumer = KafkaConsumer(
                    TOPIC_FO_REQUESTS, TOPIC_WS_EVENTS,
                    bootstrap_servers=self.bootstrap,
                    group_id="wm_central",
                    auto_offset_reset="latest", #Consume solo los mensajes nuevos, no los antiguos
                    enable_auto_commit=True,
                    value_deserializer=lambda b: b,
                )
                for rec in consumer:
                    try:
                        msg = json.loads(rec.value.decode("utf-8"))
                    except (ValueError, UnicodeDecodeError):
                        self.log(f"Mensaje inválido en {rec.topic}, descartado")
                        continue
                    if isinstance(msg, dict):
                        handler(rec.topic, msg)
            except Exception as e:
                self.log(f"Consumidor de Kafka falló: {e}, reintentando en 3 segundos...")
                time.sleep(3)

class Station:
    def __init__(self, ws_id, location, blocked=False):
        self.ws_id = ws_id
        self.location = location
        self.blocked = blocked
        self.conn = None #MonitorConnection mientras esté conectada, None si no lo está
        self.leak = None
        self.flow = 0.0
        self.volume = 0.0
        self.session = None #Sesión de riego actual, None si no hay riego
        self.pending = None

    @property
    def connected(self):
        return self.conn is not None

    @property
    def status(self):
        if not self.connected:
            return OFFLINE
        if self.leak:
            return LEAK
        if self.blocked:
            return OUT_OF_SERVICE
        if self.session:
            return WATERING
        return AVAILABLE

def normalize_duration(value):
    try:
        d = int(value)
    except (TypeError, ValueError):
        return DEFAULT_DURATION
    return d if 0 < d <= MAX_DURATION else DEFAULT_DURATION

class Central: # Clase central que contiene la DB y KAFKA bus
    def __init__(self, db, bus):
        self.db = db
        self.bus = bus
        self.lock = threading.RLock()
        self.stations = {}
        self.messages = deque(maxlen=200)
        self._load_stations()

####FUNCIONES ÚTILES
    def log(self, text): # Escribir actividades en el log
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {text}"
        with self.lock:
            self.messages.append(line)
        print(line, flush=True)

    def _publish(self, topic, payload): #Mandar un mensaje a Kafka bus
        try:
            self.bus.publish(topic, payload)
        except Exception as e:
            self.log(f"Error publicando en {topic}: {e}")

    def _load_stations(self): #Al iniciar las WS conocidas deben apareces DESCONECTADAS hasta que Monitor se conecte
        for row in self.db.query("SELECT id, location, blocked FROM watering_stations"):
            self.stations[row["id"]] = Station(row["id"], row["location"], bool(row["blocked"]))
        self.log(f"Central iniciada: {len(self.stations)} WS en BD")

    def add_operators(self, ids):
        for op in ids:
            op = op.strip()
            if op:
                self.db.execute("INSERT OR IGNORE INTO operators(id, name) VALUES (?, ?)", (op, op))
            self.log(f"Operarios dados de alta: "+", " .join(r["id"] for r in self.db.query("SELECT id FROM operators ORDER BY id")))

    def operator_exists(self, id):
        return bool(self.db.query("SELECT 1 FROM operators WHERE id = ?", (id,)))

####Eventos para FO
    def fo_event(self, operator_id, request_id, ws_id, event, msg, final=False, log=True, **extra):
        if log:
            self.log(f"[{operator_id}] {ws_id}: {msg}")
        if operator_id in INTERNAL_OPERATORS:
            return
        payload = {"request_id": request_id, "operator_id": operator_id, "ws_id": ws_id,
                       "event": event, "message": msg, "final": final, "ts": time.time()}
        payload.update(extra)
        self._publish(TOPIC_FO_EVENTS, payload)

    def deny(self, operator_id, request_id, ws_id, message):
        self.fo_event(operator_id, request_id, ws_id, "DENIED", f"Riego DENEGADO: {message}", final=True)

    def send_command(self, ws_id, command, **extra):
        payload = {"ws_id": ws_id, "command": command, "ts": time.time()}
        payload.update(extra)
        self._publish(TOPIC_WS_COMMANDS, payload)

####Estados del WS
    def snapshot(self):
        with self.lock:
            out = []
            for st in sorted(self.stations.values(), key=lambda s: s.ws_id):
                s = st.session
                out.append({
                    "ws_id": st.ws_id,
                    "location": st.location,
                    "status": st.status,
                    "flow": round(st.flow, 1) if s else 0.0,
                    "volume": round(st.volume, 1) if s else 0.0,
                    "operator_id": s["operator_id"] if s else None,
                    "elapsed": int(time.time() - s["started"]) if s else 0,
                })
            return out

    def publish_status(self):
        self._publish(TOPIC_WS_STATUS, {"ts": time.time(), "stations": self.snapshot()})

    def panel_state(self):
        stations = self.snapshot()
        summary = {}
        for s in stations:
            summary[s["status"]] = summary.get(s["status"], 0) + 1
        with self.lock:
            messages = list(self.messages)[-80:]
        return {"stations": stations, "summary": summary, "messages": messages}
 
    def unavailable_reason(self, st):
        if not st.connected:
            return "la WS está desconectada"
        if st.leak:
            return "la WS presenta una fuga"
        if st.blocked:
            return "la WS está fuera de servicio"
        if st.session or st.pending:
            return "la WS está ocupada con otro riego"
        return None

###Registrar/autenticar WS
    def register_ws(self, ws_id, location, conn):
        with self.lock:
            st = self.stations.get(ws_id)
            new = st is None
            if new:
                st = Station(ws_id, location)
                self.stations[ws_id] = st
                self.db.execute(
                    "INSERT INTO watering_stations(id, location, blocked, registered_at) VALUES(?,?,0,?)",
                    (ws_id, location, datetime.now().isoformat(timespec="seconds")))
            old = st.conn
            st.conn = conn
            conn.ws_id = ws_id
            st.leak = False
            st.flow = st.volume = 0.0
            if old is not None and old is not conn:
                self.log(f"{ws_id}: nueva conexión sustituye a una anterior")
                old.close()
            if new:
                self.log(f"{ws_id}: ALTA de nueva WS en '{location}'")
            else:
                self.log(f"{ws_id}: autenticada y conectada ('{st.location}')")
            if st.blocked:
                self.send_command(ws_id, "BLOCK")
            self.publish_status()
            return True, ("alta" if new else "autenticada")
 
    def ws_disconnected(self, ws_id, conn):
        with self.lock:
            st = self.stations.get(ws_id)
            if st is None or st.conn is not conn:
                return  # ya fue sustituida por otra conexión
            st.conn = None
            st.leak = False
            self.log(f"{ws_id}: DESCONECTADA")
            self._abort_pending(st, "la WS se ha desconectado")
            self._close_session(st, "DESCONEXION")
            st.flow = st.volume = 0.0
            self.publish_status()

####Mensajes de monitor
    def handle_monitor_message(self, text, conn):
        parts = text.split("#")
        op = parts[0].strip().upper()
        if op == "REG":
            if len(parts) < 3 or not parts[1].strip() or not parts[2].strip():
                return "KO#formato esperado REG#id#ubicacion", False
            ok, info = self.register_ws(parts[1].strip(), parts[2].strip(), conn)
            return (f"OK#{info}" if ok else f"KO#{info}"), ok
        if conn.ws_id is None:
            return "KO#no autenticado", False
        if len(parts) > 1 and parts[1].strip() != conn.ws_id:
            return "KO#id no coincide con la sesión", True
        if op == "LEAK":
            self.on_leak(conn.ws_id)
            return "OK", True
        if op == "LEAK_END":
            self.on_leak_end(conn.ws_id)
            return "OK", True
        return f"KO#operación desconocida {op}", True
 
    def on_leak(self, ws_id):
        with self.lock:
            st = self.stations.get(ws_id)
            if st is None:
                return
            st.leak = True
            self.log(f"{ws_id}: FUGA detectada")
            self._abort_pending(st, "fuga detectada en la WS")
            if st.session:
                self._close_session(st, "FUGA")
                self.send_command(ws_id, "STOP", reason="FUGA")
            self.publish_status()
 
    def on_leak_end(self, ws_id):
        with self.lock:
            st = self.stations.get(ws_id)
            if st is None:
                return
            st.leak = False
            self.log(f"{ws_id}: avería resuelta, la WS vuelve a estar {st.status}")
            self.publish_status()

####Eventos de riego
    def start_irrigation(self, request_id, operator_id, ws_id, duration):
        with self.lock:
            duration = normalize_duration(duration)
            self.fo_event(operator_id, request_id, ws_id, "STEP",
                          "Solicitud de riego recibida en CENTRAL")
            if operator_id not in INTERNAL_OPERATORS and not self.operator_exists(operator_id):
                return self.deny(operator_id, request_id, ws_id, "operario no registrado")
            st = self.stations.get(ws_id) if isinstance(ws_id, str) else None
            if st is None:
                return self.deny(operator_id, request_id, ws_id, "WS desconocida")
            self.fo_event(operator_id, request_id, ws_id, "STEP", "Operario validado")
            reason = self.unavailable_reason(st)
            if reason:
                return self.deny(operator_id, request_id, ws_id, reason)
            self.fo_event(operator_id, request_id, ws_id, "STEP",
                          "WS disponible. Solicitando autorización a la estación")
            timer = threading.Timer(AUTH_TIMEOUT, self._auth_timeout, args=(ws_id, request_id))
            timer.daemon = True
            st.pending = {"request_id": request_id, "operator_id": operator_id,
                          "duration": duration, "timer": timer}
            timer.start()
            self.send_command(ws_id, "START", request_id=request_id,
                              operator_id=operator_id, duration=duration)

    def _auth_timeout(self, ws_id, request_id):
        with self.lock:
            st = self.stations.get(ws_id)
            if st and st.pending and st.pending["request_id"] == request_id:
                self._abort_pending(st, "la WS no respondió a la solicitud de autorización")
                self.send_command(ws_id, "STOP", reason="TIMEOUT")
 
    def _abort_pending(self, st, message):
        p = st.pending
        if not p:
            return
        p["timer"].cancel()
        st.pending = None
        self.deny(p["operator_id"], p["request_id"], st.ws_id, message)
 
    def _open_session(self, st, request_id, operator_id, duration):
        st.session = {"request_id": request_id, "operator_id": operator_id,
                      "started": time.time(), "duration": duration}
        st.flow = st.volume = 0.0
 
    def _close_session(self, st, reason, volume=None):
        s = st.session
        if not s:
            return
        if volume is not None:
            st.volume = max(st.volume, float(volume))
        ended = time.time()
        duration = ended - s["started"]
        total = st.volume
        st.session = None
        st.flow = st.volume = 0.0
        self.db.execute(
            "INSERT INTO irrigations(ws_id, operator_id, request_id, started_at, ended_at,"
            " duration_s, volume_l, end_reason) VALUES(?,?,?,?,?,?,?,?)",
            (st.ws_id, s["operator_id"], s["request_id"],
             datetime.fromtimestamp(s["started"]).isoformat(timespec="seconds"),
             datetime.fromtimestamp(ended).isoformat(timespec="seconds"),
             round(duration, 1), round(total, 1), reason))
        text = REASON_TEXT.get(reason, reason)
        self.fo_event(s["operator_id"], s["request_id"], st.ws_id, "SUMMARY",
                      f"Riego finalizado ({text}): {total:.1f} L en {duration:.0f} s",
                      final=True, reason=reason, volume=round(total, 1),
                      duration=round(duration, 1))

####Eventos de WS
    def on_ws_event(self, msg):
        ws_id = msg.get("ws_id")
        event = str(msg.get("event", "")).upper()
        with self.lock:
            st = self.stations.get(ws_id)
            if st is None:
                return
            if event == "STARTED":
                self._on_started(st, msg)
            elif event == "DENIED":
                p = st.pending
                if p and p["request_id"] == msg.get("request_id"):
                    p["timer"].cancel()
                    st.pending = None
                    self.deny(p["operator_id"], p["request_id"], ws_id,
                              msg.get("message") or "la WS ha rechazado el riego")
            elif event == "FLOW":
                self._on_flow(st, msg)
            elif event == "FINISHED":
                if st.session:
                    self._close_session(st, str(msg.get("reason", "TIME")).upper(),
                                        msg.get("volume"))
                    self.publish_status()
            elif event in ("BLOCKED", "ACTIVATED"):
                self.log(f"{ws_id}: la estación confirma {event}")
 
    def _on_started(self, st, msg):
        p = st.pending
        if p and p["request_id"] == msg.get("request_id"):
            p["timer"].cancel()
            st.pending = None
            self._open_session(st, p["request_id"], p["operator_id"], p["duration"])
            self.fo_event(p["operator_id"], p["request_id"], st.ws_id, "AUTHORIZED",
                          f"Riego AUTORIZADO en {st.ws_id}: electroválvula abierta "
                          f"(límite {p['duration']} s)", duration=p["duration"])
        elif str(msg.get("operator_id", "")).upper() == "MANUAL" \
                and self.unavailable_reason(st) is None:
            self._open_session(st, str(uuid.uuid4()), "MANUAL", msg.get("duration"))
            self.log(f"{st.ws_id}: riego MANUAL iniciado en la propia estación")
        else:
            self.log(f"{st.ws_id}: STARTED ignorado (sin petición pendiente)")
            return
        self.publish_status()
 
    def _on_flow(self, st, msg):
        s = st.session
        if not s:
            return
        try:
            st.flow = float(msg.get("flow", 0))
            st.volume = float(msg.get("volume", st.volume))
        except (TypeError, ValueError):
            return
        elapsed = int(time.time() - s["started"])
        self.fo_event(s["operator_id"], s["request_id"], st.ws_id, "PROGRESS",
                      f"{st.flow:.1f} L/min - {st.volume:.1f} L acumulados",
                      log=False, flow=st.flow, volume=st.volume, elapsed=elapsed)

####Ordenes Central
    def block(self, ws_id):
        with self.lock:
            st = self.stations[ws_id]
            st.blocked = True
            self.db.execute("UPDATE watering_stations SET blocked = 1 WHERE id = ?", (ws_id,))
            self._abort_pending(st, "la WS ha sido puesta fuera de servicio")
            self._close_session(st, "BLOQUEO")
            self.send_command(ws_id, "BLOCK")
            self.log(f"{ws_id}: puesta FUERA DE SERVICIO por CENTRAL")
            self.publish_status()
 
    def activate(self, ws_id):
        with self.lock:
            st = self.stations[ws_id]
            st.blocked = False
            self.db.execute("UPDATE watering_stations SET blocked = 0 WHERE id = ?", (ws_id,))
            self.send_command(ws_id, "ACTIVATE")
            self.log(f"{ws_id}: ACTIVADA por CENTRAL ({st.status})")
            self.publish_status()
 
    def admin_command(self, action, ws_id, duration=None):
        action = str(action).upper()
        if action not in ("START", "BLOCK", "ACTIVATE"):
            return False, "Acción no válida"
        with self.lock:
            if ws_id == "ALL":
                targets = sorted(self.stations)
            elif ws_id in self.stations:
                targets = [ws_id]
            else:
                return False, f"WS {ws_id} desconocida"
            if not targets:
                return False, "No hay WS registradas"
            self.log(f"CENTRAL: orden {action} sobre {ws_id}")
            for t in targets:
                if action == "START":
                    self.start_irrigation(str(uuid.uuid4()), "CENTRAL", t, duration)
                elif action == "BLOCK":
                    self.block(t)
                else:
                    self.activate(t)
        return True, f"Orden {action} enviada a {len(targets)} WS"


####Entrada Kafka
    def on_kafka(self, topic, msg):
        try:
            if topic == TOPIC_FO_REQUESTS:
                self._on_fo_request(msg)
            elif topic == TOPIC_WS_EVENTS:
                self.on_ws_event(msg)
        except Exception as e:
            self.log(f"Error procesando mensaje de {topic}: {e!r}")
 
    def _on_fo_request(self, msg):
        operator_id = str(msg.get("operator_id", ""))
        request_id = str(msg.get("request_id") or uuid.uuid4())
        ws_id = msg.get("ws_id")
        if str(msg.get("action", "")).upper() != "START" or not operator_id or not ws_id:
            self.fo_event(operator_id or "?", request_id, ws_id, "DENIED",
                          "Petición mal formada (action, operator_id y ws_id obligatorios)",
                          final=True)
            return
        self.start_irrigation(request_id, operator_id, ws_id, msg.get("duration"))

####Servidor sockets
    def start_socket_server(self, port):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", port))
        srv.listen(50)
        threading.Thread(target=self._accept_loop, args=(srv,), daemon=True).start()
        return srv.getsockname()[1]
 
    def _accept_loop(self, srv):
        while True:
            try:
                sock, addr = srv.accept()
            except OSError:
                break
            threading.Thread(target=self._serve_monitor, args=(sock, addr), daemon=True).start()
 
    def _serve_monitor(self, sock, addr):
        sock.settimeout(IDLE_TIMEOUT)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        mc = MonitorConnection(sock)
        try:
            if mc.read_byte() != ENQ:
                sock.sendall(bytes([NACK]))
                return
            sock.sendall(bytes([ACK]))
            while True:
                b = mc.read_byte()
                if b == EOT:
                    break
                if b != STX:
                    continue
                data, received_lrc = mc.read_frame_body()
                if lrc(data) != received_lrc:
                    sock.sendall(bytes([NACK]))
                    continue
                sock.sendall(bytes([ACK]))
                answer, keep = self.handle_monitor_message(
                    data.decode("utf-8", errors="replace"), mc)
                self._send_answer(mc, answer)
                if not keep:
                    break
        except (ConnectionError, EOFError, OSError):
            pass
        except Exception as e:
            self.log(f"Error en sesión con {addr}: {e!r}")
        finally:
            if mc.ws_id:
                self.ws_disconnected(mc.ws_id, mc)
            mc.close()
 
    @staticmethod
    def _send_answer(mc, text):
        frame = build_frame(text)
        for _ in range(3):
            mc.sock.sendall(frame)
            b = mc.read_byte()
            if b == EOT:
                raise EOFError
            if b != NACK:
                return


PANEL_HTML = r"""<!doctype html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WM_Central</title>
<style>
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;font-family:system-ui,sans-serif;background:#0b1620;color:#dfe9f0}
header{padding:14px 16px;border-bottom:1px solid #1f3342}
h1{margin:0 0 10px;font-size:20px} h1 span{font-weight:400;color:#8aa3b5}
h2{margin:0 0 8px;font-size:14px;color:#8aa3b5;text-transform:uppercase;letter-spacing:.05em}
main{padding:16px;display:grid;gap:16px;max-width:1100px;margin:0 auto}
.card{background:#10212e;border:1px solid #1f3342;border-radius:10px;padding:14px}
.pill{display:inline-block;padding:3px 10px;border-radius:999px;margin:0 6px 6px 0;font-size:13px;font-weight:600;color:#fff}
.bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:12px}
input{width:80px;padding:6px;border-radius:6px;border:1px solid #2a4458;background:#0b1620;color:inherit}
button{padding:6px 12px;border-radius:6px;border:1px solid #2a4458;background:#183247;color:inherit;cursor:pointer}
button:hover{background:#21465f}
.wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:640px}
th,td{padding:8px;border-bottom:1px solid #1f3342;text-align:left;font-size:14px}
th{color:#8aa3b5;font-weight:600} td.num{font-variant-numeric:tabular-nums}
.acts{white-space:nowrap} .acts button{margin-right:4px;padding:4px 8px;font-size:12px}
.st{padding:4px 10px;border-radius:6px;font-weight:700;text-align:center;color:#fff;min-width:130px}
.ok{background:#2e9e4f}
.watering{background:#2e9e4f;animation:blink 1s infinite}
@keyframes blink{50%{background:#9be8b2;color:#0b1620}}
.leak{background:#d63b3b}
.oos{background:#e8891c}
.off{background:#6b7480}
pre{margin:0;max-height:260px;overflow:auto;font-size:12px;line-height:1.5;background:#08121a;padding:10px;border-radius:6px;white-space:pre-wrap}
#flash{color:#9be8b2;font-size:13px}
</style></head>
<body>
<header><h1>WaterManagement <span>· Panel de CENTRAL</span></h1><div id="pills"></div></header>
<main>
<section class="card">
<div class="bar">
<label>Duración riego (s) <input id="dur" type="number" min="1" max="3600" value="30"></label>
<button data-a="START" data-w="ALL">Regar todas</button>
<button data-a="BLOCK" data-w="ALL">Bloquear todas</button>
<button data-a="ACTIVATE" data-w="ALL">Activar todas</button>
<span id="flash"></span>
</div>
<div class="wrap"><table>
<thead><tr><th>ID</th><th>Ubicación</th><th>Estado</th><th>Caudal (L/min)</th><th>Volumen (L)</th><th>Operario</th><th>Acciones</th></tr></thead>
<tbody id="rows"></tbody></table></div>
</section>
<section class="card"><h2>Mensajes de la aplicación</h2><pre id="log"></pre></section>
</main>
<script>
let token = sessionStorage.getItem('wm_token') || '';
let timer = null;
const LABEL = {DISPONIBLE:'Disponible', REGANDO:'Regando', FUGA:'Fuga', FUERA_DE_SERVICIO:'Fuera de Servicio', DESCONECTADA:'Desconectada'};
const CLS = {DISPONIBLE:'ok', REGANDO:'watering', FUGA:'leak', FUERA_DE_SERVICIO:'oos', DESCONECTADA:'off'};
const $ = id => document.getElementById(id);
function el(tag, cls, text){const e=document.createElement(tag); if(cls) e.className=cls; if(text!==undefined) e.textContent=text; return e;}
async function api(path, body){
  const opts = {headers:{'X-Token':token,'Content-Type':'application/json'}};
  if(body){opts.method='POST'; opts.body=JSON.stringify(body);}
  const r = await fetch(path, opts);
  if(r.status===401){
    clearInterval(timer);
    const t = prompt('Token del panel:');
    if(t){sessionStorage.setItem('wm_token',t); token=t; timer=setInterval(refresh,1000);}
    throw new Error('no autorizado');
  }
  return r.json();
}
function flash(t){$('flash').textContent=t; setTimeout(()=>{if($('flash').textContent===t) $('flash').textContent='';},4000);}
async function command(action, ws){
  try{const r=await api('/api/command',{action:action, ws_id:ws, duration:parseInt($('dur').value)||30}); flash(r.message);}
  catch(e){flash(String(e));}
  refresh();
}
function render(s){
  const pills=$('pills'); pills.replaceChildren();
  for(const k of Object.keys(LABEL)) pills.appendChild(el('span','pill '+CLS[k], LABEL[k]+': '+(s.summary[k]||0)));
  const rows=$('rows'); rows.replaceChildren();
  if(!s.stations.length){const tr=el('tr'); const td=el('td',null,'Sin estaciones registradas'); td.colSpan=7; tr.appendChild(td); rows.appendChild(tr);}
  for(const w of s.stations){
    const tr=el('tr'); const watering = w.status==='REGANDO';
    tr.appendChild(el('td',null,w.ws_id)); tr.appendChild(el('td',null,w.location));
    const td=el('td'); td.appendChild(el('div','st '+(CLS[w.status]||''), LABEL[w.status]||w.status)); tr.appendChild(td);
    tr.appendChild(el('td','num', watering? w.flow.toFixed(1):'-'));
    tr.appendChild(el('td','num', watering? w.volume.toFixed(1):'-'));
    tr.appendChild(el('td',null, watering? w.operator_id:'-'));
    const act=el('td','acts');
    for(const [a,l] of [['START','Regar'],['BLOCK','Bloquear'],['ACTIVATE','Activar']]){
      const b=el('button',null,l); b.onclick=()=>command(a,w.ws_id); act.appendChild(b);
    }
    tr.appendChild(act); rows.appendChild(tr);
  }
  const log=$('log'); const atEnd = log.scrollTop+log.clientHeight >= log.scrollHeight-20;
  log.textContent = s.messages.join('\n'); if(atEnd) log.scrollTop=log.scrollHeight;
}
async function refresh(){ try{ render(await api('/api/state')); }catch(e){} }
document.querySelectorAll('button[data-a]').forEach(b=>b.onclick=()=>command(b.dataset.a,b.dataset.w));
refresh(); timer=setInterval(refresh,1000);
</script></body></html>
"""

class PanelHandler(BaseHTTPRequestHandler):
    central = None
    token = None
 
    def log_message(self, *args):
        pass
 
    def _send(self, code, body, ctype="application/json"):
        if not isinstance(body, bytes):
            body = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
 
    def _authorized(self):
        if not self.token:
            return True
        return hmac.compare_digest(self.headers.get("X-Token", ""), self.token)
 
    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/":
            self._send(200, PANEL_HTML.encode("utf-8"), "text/html")
        elif path == "/api/state":
            if not self._authorized():
                return self._send(401, {"error": "no autorizado"})
            self._send(200, self.central.panel_state())
        else:
            self._send(404, {"error": "no encontrado"})
 
    def do_POST(self):
        if self.path.split("?")[0] != "/api/command":
            return self._send(404, {"error": "no encontrado"})
        if not self._authorized():
            return self._send(401, {"error": "no autorizado"})
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length > 4096:
                return self._send(413, {"error": "petición demasiado grande"})
            body = json.loads(self.rfile.read(length) or b"{}")
            ok, message = self.central.admin_command(
                body.get("action"), str(body.get("ws_id", "")), body.get("duration"))
            self._send(200 if ok else 400, {"ok": ok, "message": message})
        except (ValueError, TypeError):
            self._send(400, {"ok": False, "message": "JSON no válido"})

def start_panel(central, port, token=None):
    handler = type("BoundPanelHandler", (PanelHandler,), {"central": central, "token": token})
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]

def main():
    env = os.environ.get
    p = argparse.ArgumentParser(prog=APP_NAME, description="Sistema central de WaterManagement")
    p.add_argument("socket_port", nargs="?", type=int, default=int(env("SOCKET_PORT", 5000)),
                   help="Puerto de escucha del servidor de sockets (Monitor de las WS)")
    p.add_argument("broker", nargs="?", default=env("KAFKA_BOOTSTRAP"),
                   help="IP:puerto del broker/bootstrap-server de Kafka")
    p.add_argument("--db", default=env("DB_PATH", "/app/data/wm_central.db"))
    p.add_argument("--web-port", type=int, default=int(env("PORT", 8080)))
    p.add_argument("--operators", default=env("OPERATORS", ""))
    p.add_argument("--panel-token", default=env("PANEL_TOKEN"))
    args = p.parse_args()
    if not args.broker:
        p.error("falta el broker de Kafka (IP:puerto o variable KAFKA_BOOTSTRAP)")
 
    db = DataBase(args.db)
    boot_log = lambda m: print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)
 
    bus = None
    while bus is None:
        try:
            bus = KafkaBus(args.broker, boot_log)
        except Exception as e:
            boot_log(f"Esperando a Kafka en {args.broker} ({e})")
            time.sleep(3)
    bus.ensure_topics()
 
    central = Central(db, bus)
    bus.log = central.log
    central.add_operators(args.operators.split(","))
 
    threading.Thread(target=bus.consume_forever, args=(central.on_kafka,), daemon=True).start()
 
    def status_loop():
        while True:
            central.publish_status()
            time.sleep(STATUS_PERIOD)
    threading.Thread(target=status_loop, daemon=True).start()
 
    sock_port = central.start_socket_server(args.socket_port)
    web_port = start_panel(central, args.web_port, args.panel_token)
    central.log(f"Sockets en :{sock_port} | Panel web en :{web_port} | Kafka en {args.broker}")
 
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        central.log("CENTRAL detenida")
 
 
if __name__ == "__main__":
    main()