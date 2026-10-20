import argparse
import hmac
import json
import os
import socket
import sqlite3
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime

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
    def __init(self, sock):
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
                    id TEXT PRIMARY KEY,
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
                    end_reason  TEXT,
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
    def __init__(self, bootstrap, log): #inicializar kafka producer con bootstrap y log, con ack=all y retries=3
        from kafka import KafkaProducer
        self.bootstrap = bootstrap
        self.log = log
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v.encode('utf-8')), #Crear un productor de Kafka que convierte los mensajes a JSON y los codifica en UTF-8
            ack="all",
            retries=3,
        )

    def esure_topics(self): #Crear los topics de Kafka si no existen, ignorando el error si ya existen
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

class Central:
    def __innit__(self, db, bus):
        self.db = db
        self.bus = bus
        self.lock = threading.RLock()
        self.stations = {}
        self.messages = deque(maxlen=200)
        self._load_stations()

####FUNCIONES ÚTILES
def log(self, text):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {text}"
    with self.lock:
        self.messages.append(line)
    print(line, flush=True)

def _publish(self, topic, payload):
    try:
        self.bus.publish(topic, payload)
    except Exception as e:
        self.log(f"Error publicando en {topic}: {e}")

def _load_stations(self): #Al iniciar las WS conocidas deben apareces DESCONECTADAS hasta que Monitor se conecte
    for row in self.db.query("SELECT id, location, blocked FROM watering_stations"):
        self.station[row["id"]] = Station(row["id"], row["location"], bool(row["blocked"]))
    self.log(f"Central iniciada: {len(self.stations)} WS en BD")

def add_operators(self, ids):
    for op in ids:
        op = op.strip()
        if op:
            self.db.execute("INSERT OR IGNORE INTO operators(id, name) VALUES (?, ?)", (op, op))
        self.log(f"Operarios dados de alta: "+", " .join(r["id"] for r in self.db.query("SELECT id FROM operators ORDER BY id")))

def operator_exists(self, id):
    return bool(self.db.query("SELECT 1 FROM operators WHERE id = ?", (id,)))

