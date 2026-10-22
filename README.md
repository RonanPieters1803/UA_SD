# SD-Water-Management-Network
# Ronan Pieters
# Jorge Campello Jiménez

## Formato de mensajes Kafka

Los mensajes de los topics `riego.comandos` y `riego.telemetria` se transmiten
como objetos JSON codificados en UTF-8.

Mensaje de central --> Engine
En `riego.comandos`, cada mensaje contiene `instruccion` e `id_operario`.
Para `START` se puede incluir `tiempo` (duración en segundos); si se omite o
se envía como `null`, el riego continúa sin límite de tiempo. Ejemplos:

```json
{"instruccion":"ON","id_operario":"op-123"}
{"instruccion":"START","id_operario":"op-123","tiempo":30}
{"instruccion":"START","id_operario":"op-123"}
{"instruccion":"STOP","id_operario":"op-123"}
```

Mensaje de engine--> central
En `riego.telemetria`, el engine publica `id`, `caudal_l_min`,
`volumen_acumulado_l` e `id_operario`. Los campos de caudal y volumen son
números JSON.