### SD-Water-Management-Network
Ronan Pieters
Jorge Campello Jimenez


### CENTRAL

- Se conecta al broker Kafka, crea los cinco topics del sistema cuando puede
  y consume solicitudes de operarios y eventos de los Engines.
- Recibe el registro de cada Monitor por TCP (`REG#<id>#<ubicación>`). Las
  estaciones conocidas se guardan en SQLite; al reiniciar CENTRAL empiezan
  desconectadas hasta que su Monitor vuelve a registrarlas.
- Valida cada solicitud: el operario debe estar dado de alta y la estación
  debe estar conectada, sin fuga, habilitada y sin otro riego activo o
  pendiente. La duración válida está entre 1 y 3600 segundos; si falta o no
  es válida se usa el valor por defecto de 30 segundos.
- Al aceptar una solicitud, envía `START` al Engine y espera su evento
  `STARTED`. Si no recibe autorización en 10 segundos, cancela la solicitud.
  Durante el riego actualiza el estado y comunica el progreso y el resumen
  final al operario.
- Publica el estado de las estaciones cada 2 segundos. Guarda operadores,
  estaciones, bloqueos y el historial de riegos en SQLite.
- Expone un panel web en `/` y una API: `GET /api/state` y
  `POST /api/command`. El panel permite regar, bloquear o activar una estación
  o todas. Si se configura `PANEL_TOKEN`, las llamadas a la API requieren el
  encabezado `X-Token`.

Estados posibles de una estación:

| Estado | Significado |
| --- | --- |
| `DESCONECTADA` | El Monitor no tiene una conexión activa con CENTRAL. |
| `FUGA` | El Monitor ha detectado una avería en el Engine. |
| `FUERA_DE_SERVICIO` | CENTRAL ha bloqueado la estación. |
| `REGANDO` | Hay una sesión de riego activa. |
| `DISPONIBLE` | La estación está conectada y puede aceptar una solicitud. |

### Monitor

- Recibe como parámetros su puerto local, el host y puerto de CENTRAL, el ID
  de la estación y su ubicación.
- Mantiene una conexión TCP con CENTRAL, hace el handshake y registra la
  estación. Reintenta la conexión si CENTRAL no está disponible.
- Escucha conexiones del Engine en su puerto local. Envía un heartbeat cada
  segundo y comprueba su respuesta: informa `LEAK` si el Engine devuelve `KO`
  o deja de responder, y `LEAK_END` cuando se recupera.
- Envía `PING#<id>` a CENTRAL cada 15 segundos para mantener activa la
  conexión. CENTRAL considera inactiva la conexión tras 30 segundos sin
  tráfico.
- Los enlaces TCP usan tramas con `STX`, datos UTF-8, `ETX` y un LRC calculado
  con XOR; el intercambio con CENTRAL también utiliza ACK.

### Engine

- Se conecta al Monitor, recibe el ID de estación y responde a sus heartbeats.
  Después se conecta a Kafka y escucha `wm_ws_commands`.
- Solo procesa órdenes dirigidas a su ID (también admite órdenes para `ALL`).
  `START` inicia el riego; `STOP` lo detiene; `BLOCK` lo pone fuera de
  servicio; `ACTIVATE` vuelve a habilitarlo.
- Durante el riego publica un evento `FLOW` por segundo con caudal y volumen
  acumulado. Al terminar publica `FINISHED` con la duración, el volumen y el
  motivo de fin.
- El caudal configurado al arrancar se expresa en litros por minuto. El
  Engine también ofrece un menú local para pruebas (`start`, `stop`, `on`,
  `off`, `averia` y `recuperar`). En ese menú, `start` sin duración mantiene
  el riego hasta recibir `stop`; se puede indicar una duración en segundos.

### Operario (FO)

- Se conecta al mismo broker Kafka e inicia sesión con un ID previamente dado
  de alta en CENTRAL.
- Envía solicitudes `START` para una estación y recibe sus eventos de
  validación, autorización, progreso, denegación y resumen final.
- En modo interactivo acepta `ID_ESTACION [DURACION_SEGUNDOS]`, `ls` (o
  `estado`) para ver el último estado recibido, y `salir`.
- Puede procesar primero un fichero de servicios con `--file`; envía sus
  solicitudes en secuencia, espera la respuesta final de cada una y deja
  cuatro segundos entre solicitudes. Después pasa al modo interactivo.

## Kafka: topics y mensajes

| Topic | Dirección | Uso |
| --- | --- | --- |
| `wm_fo_requests` | FO → CENTRAL | Solicitudes de riego. |
| `wm_fo_events` | CENTRAL → FO | Validación, autorización, progreso y resultado. |
| `wm_ws_commands` | CENTRAL → Engine | Órdenes para una estación. |
| `wm_ws_events` | Engine → CENTRAL | Autorización, telemetría y fin de riego. |
| `wm_ws_status` | CENTRAL → consumidores | Estado periódico de las estaciones. |

Solicitud de riego enviada por FO:

```json
{
  "action": "START",
  "operator_id": "op-123",
  "ws_id": "WS_1",
  "duration": 30,
  "request_id": "identificador-de-solicitud",
  "ts": 1730000000.0
}
```

CENTRAL envía al Engine una orden equivalente con `command`, `ws_id`,
`request_id`, `operator_id` y `duration`. El Engine publica eventos con
`ws_id`, `event` y `request_id`: `STARTED`, `DENIED`, `FLOW` o `FINISHED`.
`FLOW` incluye `flow` (L/min) y `volume` (litros); `FINISHED` incluye
`reason`, `duration` y `volume`.

Los eventos de `wm_fo_events` incluyen `operator_id`, `ws_id`, `event`,
`message`, `request_id` y `final`. `final: true` indica que la solicitud
terminó, tanto si fue denegada como si el riego finalizó. El topic
`wm_ws_status` publica un objeto con `stations`; cada estación informa, entre
otros campos, `ws_id`, `location`, `status`, `flow`, `volume` y `operator_id`.

