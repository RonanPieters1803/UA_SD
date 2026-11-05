import json, argparse, threading, time, uuid, re
from datetime import datetime
 
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

#Nombres de los topics de Kafka
TOPIC_FO_REQUESTS = "wm_fo_requests" #FO -> CENTRAL
TOPIC_FO_EVENTS = "wm_fo_events"     #CENTRAL -> FO
TOPIC_WS_STATUS = "wm_ws_status"     #CENTRAL -> TODOS
DEFAULT_DURATION = 30

ARRANQUE = time.time()

lock = threading.Lock()
pendientes = {}   # request_id -> Event (se activa con el evento final)
estaciones = []   # ultimo estado publicado por CENTRAL

def log(msg):
    print(f"[WM_FO] {datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)

def crear_consumidor(broker, operator_id):
    topics = [TOPIC_FO_EVENTS, TOPIC_WS_STATUS]
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=broker,
                group_id=f"fo-{operator_id}", 
                enable_auto_commit=False,
                auto_offset_reset="latest")
            tps = []
            for t in topics:
                parts = consumer.partitions_for_topic(t)
                if not parts:
                    raise RuntimeError(f"el topic {t} aun no existe (CENTRAL arrancada?)")
                tps += [TopicPartition(t, p) for p in sorted(parts)]
            consumer.assign(tps)
            fin = consumer.end_offsets(tps)
            for tp in tps:
                if tp.topic == TOPIC_FO_EVENTS:
                        committed = consumer.committed(tp)
                        consumer.seek(tp, committed if committed is not None else fin[tp])
                elif tp.topic == TOPIC_WS_STATUS:
                        consumer.seek(tp, max(0, fin[tp] - 1))
            consumer.commit()
            return consumer
        except Exception as e:
            log(f"Esperando a Kafka en {broker} ({e})")
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass
            time.sleep(3)

def escuchar(consumer, operator_id):
    global estaciones
    while True:
        try:
            for rec in consumer:
                try:
                    m = json.loads(rec.value.decode("utf-8"))
                except (ValueError, UnicodeError):
                    m = None
                if isinstance(m, dict):
                    if rec.topic == TOPIC_FO_EVENTS and m.get("operator_id") == operator_id:
                        recuperado = m.get("ts", time.time()) < ARRANQUE
                        if recuperado:
                            if m.get("final") is True:
                                log(f"[RECUPERADO] {m.get('ws_id')} | {m.get('event')}: {m.get('message', '')}")
                            else:
                                log(f"{m.get('ws_id')} | {m.get('event')}: {m.get('message', '')}")
                    if m.get("final"):
                        with lock:
                            ev = pendientes.get(m.get("request_id"))
                        if ev:
                            ev.set()
                elif rec.topic == TOPIC_WS_STATUS:
                    with lock:
                        estaciones = m.get("stations", [])
            if rec.topic in TOPIC_FO_EVENTS:
                consumer.commit()
        except Exception as e:
            log(f"Error escuchando eventos: {e}")
            time.sleep(2)

def mostrar_estaciones():
    with lock:
        st = list(estaciones)
    if not st:
        print("  (sin datos de estaciones todavia)")
        return
    for s in st:
        print(f"  {s.get('ws_id', '?'):<10} {s.get('status', '?'):<18} {s.get('location', '')}")

def leer_servicios(path):
    with open(path, encoding="utf-8") as f:
        content = f.read().strip()
    if not content:
        return []
    items = []
    try:
        data = json.loads(content)
        items = data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("{"):
                items.append(json.loads(line))
            else:
                items.append([x for x in re.split(r"[,\s;]+", line) if x])
    servicios = []
    for it in items:
        if isinstance(it, dict):
            servicios.append((str(it["ws_id"]), int(it.get("duration", DEFAULT_DURATION))))
        elif isinstance(it, (list, tuple)) and it:
            servicios.append((str(it[0]), int(it[1]) if len(it) > 1 else DEFAULT_DURATION))
        else:
            servicios.append((str(it), DEFAULT_DURATION))
    return servicios

def main():
    p = argparse.ArgumentParser(description="Cliente de operario WaterManagement")
    p.add_argument("broker", help="Kafka bootstrap, por ejemplo localhost:9092")
    p.add_argument("operator_id", help="ID del operario dado de alta en CENTRAL")
    p.add_argument("--file", dest="services_file", help="JSON/JSONL con solicitudes")
    args = p.parse_args()
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.broker,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all", retries=3)
        except Exception as e:
            log(f"Esperando a Kafka en {args.broker} ({e})")
            time.sleep(3)
    consumer = crear_consumidor(args.broker, args.operator_id)
    threading.Thread(target=escuchar, args=(consumer, args.operator_id), daemon=True).start()
    log(f"Operario {args.operator_id} conectado a Kafka ({args.broker}).")

    def send(ws_id, duration):
        rid = str(uuid.uuid4())
        ev = threading.Event()
        with lock:
            pendientes[rid] = ev
        msg = {"action": "START", "operator_id": args.operator_id, "ws_id": ws_id,
               "duration": int(duration), "request_id": rid, "ts": time.time()}
        producer.send(TOPIC_FO_REQUESTS, msg).get(timeout=10)
        log(f"Solicitud enviada: estacion={ws_id}, duracion={duration}s")
        return ev

    try:
        if args.services_file:
            for ws_id, dur in leer_servicios(args.services_file):
                ev = send(ws_id, dur)
                # Esperar a la conclusion (exito o fracaso) y 4 s antes del siguiente servicio
                if not ev.wait(timeout=dur + 30):
                    log(f"Sin respuesta final de CENTRAL para {ws_id}")
                time.sleep(4)
            log("Fichero de servicios completado. Modo interactivo.")
        log("Comandos: ID_ESTACION [DURACION_SEGUNDOS] | ls | salir")
        while True:
            line = input("FO> ").strip()
            if line.lower() in ("salir", "exit", "quit"):
                break
            if line.lower() in ("ls", "estado"):
                mostrar_estaciones()
                continue
            parts = line.split()
            if not parts:
                continue
            try:
                send(parts[0], int(parts[1]) if len(parts) > 1 else DEFAULT_DURATION)
            except ValueError as e:
                log(f"Entrada invalida: {e}")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()
