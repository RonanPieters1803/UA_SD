import json, argparse, os, sys, threading, time, uuid
from datetime import datetime
 
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

#Nombres de los topics de Kafka
TOPIC_FO_REQUESTS = "wm_fo_requests"
TOPIC_FO_EVENTS = "wm_fo_events" 

def log(msg):
    print(f"[WM_FO] {datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)

def main():
    p = argparse.ArgumentParser(description="Cliente de operario WaterManagement")
    p.add_argument("broker", help="Kafka bootstrap, por ejemplo localhost:9092")
    p.add_argument("operator_id", help="ID del operario dado de alta en CENTRAL")
    p.add_argument("--file", dest="services_file", help="JSON/JSONL con solicitudes")
    p.add_argument("--group", default=None, help="Grupo Kafka opcional para eventos")
    args = p.parse_args()
    producer = KafkaProducer(bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all", retries=3)
    consumer = KafkaConsumer(TOPIC_FO_EVENTS, bootstrap_servers=args.broker,
        group_id=args.group or f"wm_fo_{args.operator_id}_{uuid.uuid4().hex[:8]}",
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    def listen():
        for rec in consumer:
            m=rec.value
            if m.get("operator_id")==args.operator_id:
                log(f"{m.get('ws_id')} | {m.get('event')}: {m.get('message','')}")
    threading.Thread(target=listen, daemon=True).start()
    def send(ws_id, duration=30):
        msg={"action":"START","operator_id":args.operator_id,"ws_id":ws_id,
             "duration":int(duration),"request_id":str(uuid.uuid4())}
        producer.send(TOPIC_FO_REQUESTS,msg).get(timeout=10)
        log(f"Solicitud enviada: estación={ws_id}, duración={duration}s")
    try:
        if args.services_file:
            with open(args.services_file,encoding="utf-8") as f:
                content=f.read().strip()
            if not content: return
            try: items=json.loads(content)
            except json.JSONDecodeError: items=[json.loads(line) for line in content.splitlines() if line.strip()]
            if isinstance(items,dict): items=[items]
            for item in items:
                send(str(item["ws_id"]),item.get("duration",30))
                time.sleep(4)
            while True: time.sleep(1)
        else:
            log("Introduce ID_ESTACION [DURACION_SEGUNDOS], o 'salir'.")
            while True:
                line=input("FO> ").strip()
                if line.lower() in ("salir","exit","quit"): break
                parts=line.split()
                if not parts: continue
                try: send(parts[0],int(parts[1]) if len(parts)>1 else 30)
                except (ValueError,KeyError) as e: log(f"Entrada inválida: {e}")
    except (KeyboardInterrupt, EOFError): pass
    finally:
        consumer.close(); producer.close()
if __name__=="__main__": main()

