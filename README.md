# Trabajo Práctico - Coordinación (Implementación Python)

## Resumen

Se implementó una solución distribuida en Python para resolver consultas concurrentes de múltiples clientes sobre datos de frutas, escalando en cantidad de instancias de Sum y Aggregation sin depender de un número fijo de contenedores.

La solución preserva el protocolo externo y la opacidad de `FruitItem`, y reemplaza la coordinación interna por un protocolo explícito con mensajes tipados y barreras por cliente.

## Objetivos que resuelve

- Soportar múltiples clientes concurrentes sin mezclar resultados.
- Coordinar correctamente múltiples instancias de Sum frente a EOF por cliente.
- Coordinar correctamente múltiples instancias de Aggregation para emitir un parcial consistente por cliente.
- Consolidar parciales en Join y emitir un único resultado final por cliente.
- Escalar procesamiento evitando broadcast redundante entre Sum y Aggregation.
- Implementar middleware RabbitMQ funcional con ACK/NACK manual y mensajes persistentes.
- Implementar cierre graceful por SIGTERM en Sum, Aggregation y Join.

## Arquitectura de alto nivel

Flujo principal:

1. Client envía registros al Gateway por TCP.
2. Gateway serializa cada registro como `DATA(client_id, fruit, amount)`.
3. Gateway serializa fin de cliente como `EOF(client_id)`.
4. Sum recibe mensajes, acumula por cliente y fruta.
5. Sum publica barrera de cierre por cliente en `SUM_CONTROL_EXCHANGE`.
6. Cada Sum, al recibir barrera, flushea el cliente:
   - envía datos a una única instancia de Aggregation según hash estable;
   - envía `AGG_EOF_BARRIER(client_id, sum_id)` a todas las instancias de Aggregation.
7. Cada Aggregation acumula por cliente y emite parcial cuando recibió barrera de todos los Sum.
8. Join recibe parciales, mergea por cliente, y cuando junta `AGGREGATION_AMOUNT` parciales emite `RESULT(client_id, fruit_top)`.
9. Gateway entrega al socket correcto filtrando por `client_id`.

## Decisiones de diseño y justificación

### 1) Correlación por cliente con `client_id`

Se agregó `client_id` único por conexión en Gateway.

Motivo:

- Sin correlación explícita, en concurrencia es posible entregar el resultado de un cliente a otro.
- Con `client_id`, todos los mensajes internos quedan asociados a una consulta concreta.

Impacto:

- Aislamiento entre consultas concurrentes.
- Matching determinístico en la salida hacia Gateway.

### 2) Protocolo interno tipado

Se definieron tipos de mensaje internos:

- `DATA`
- `EOF`
- `SUM_EOF_BARRIER`
- `AGG_EOF_BARRIER`
- `PARTIAL_RESULT`
- `RESULT`

Motivo:

- Evita lógica implícita basada en longitud de listas.
- Mejora validación, evolución y trazabilidad.

Impacto:

- Menos ambigüedad en parsing.
- Flujo más robusto ante errores de formato.

### 3) Barrera de EOF por cliente entre Sum y Aggregation

Se implementó una coordinación por cliente en dos niveles:

- Barrera de Sum: `SUM_EOF_BARRIER(client_id)`
- Barrera de Aggregation: `AGG_EOF_BARRIER(client_id, sum_id)`

Motivo:

- En una cola compartida, el EOF de cliente puede llegar primero a una sola instancia de Sum.
- Sin barrera, una instancia podría cerrar temprano y generar resultados truncados.

Impacto:

- Todas las instancias participan del cierre de cada cliente.
- Aggregation recién emite parcial cuando conoce fin global de Sum para ese cliente.

### 4) Particionado determinístico Sum -> Aggregation

Cada fruta se enruta a un único Aggregation con hash estable:

`aggregation_id = hash_sha256(client_id + "|" + fruit) % AGGREGATION_AMOUNT`

Motivo:

- Evitar broadcast de cada fruta a todas las instancias.
- Reducir costo de CPU y ancho de banda interno.

Impacto:

- Escalabilidad horizontal real.
- Menor duplicación de trabajo.

### 5) Consolidación final en Join

Join recibe `PARTIAL_RESULT(client_id, aggregation_id, fruit_top)` y mantiene estado por cliente hasta recibir `AGGREGATION_AMOUNT` parciales únicos.

Motivo:

- Con múltiples Aggregation, no existe un único parcial final hasta combinar todos.

Impacto:

- Un solo `RESULT` por cliente.
- Top final correcto con cualquier multiplicidad de Aggregation.

### 6) Middleware RabbitMQ robusto

Se implementaron colas y exchanges con:

- declaraciones durables;
- mensajes persistentes;
- consumo manual con ACK/NACK;
- mapeo de excepciones de conexión y de mensajería;
- cierre de recursos por conexión.

Motivo:

- El esqueleto no implementaba middleware.

Impacto:

- Base funcional para escenarios de coordinación distribuidos.

### 7) Cierre graceful por SIGTERM

Se agregó manejo de SIGTERM en Sum, Aggregation y Join para detener consumo y cerrar conexiones sin finalizar abruptamente.

Motivo:

- Requisito de robustez operativa en sistemas distribuidos.

Impacto:

- Menor riesgo de corrupción de estado en apagados.

## Estado por componente

### Gateway

- Mantiene protocolo externo original.
- `message_handler` genera `client_id` por conexión.
- Serializa `DATA` y `EOF` internos.
- Filtra `RESULT` por `client_id` antes de responder al socket.

### Sum

- Acumula por `client_id` y fruta usando `FruitItem`.
- Publica barrera de cierre por cliente.
- Flushea estado del cliente hacia Aggregation usando hash estable.
- Emite barrera de fin para Aggregation con su `sum_id`.

### Aggregation

- Acumula por cliente.
- Cuenta barreras recibidas por `sum_id`.
- Emite parcial al Join cuando recibió `SUM_AMOUNT` barreras del cliente.

### Join

- Mergea parciales por cliente.
- Espera `AGGREGATION_AMOUNT` parciales únicos por cliente.
- Emite resultado final al Gateway.

## Escalabilidad

### Escalado en clientes

La correlación por `client_id` permite procesar clientes concurrentes en el mismo pipeline sin mezclar mensajes ni respuestas.

### Escalado en Sum

Cada instancia consume de la cola de entrada y participa en barrera por cliente. El cierre no depende de una única instancia.

### Escalado en Aggregation

El particionado determinístico distribuye carga entre instancias. Join recompone el resultado final independientemente de la multiplicidad.

## Correctitud de resultados

La solución protege dos invariantes:

1. Cada cliente recibe exactamente un resultado final asociado a su `client_id`.
2. Cada resultado final se emite tras completar la coordinación de cierre de todas las instancias relevantes.

## Archivos modificados

- `python/src/common/message_protocol/internal.py`
- `python/src/common/message_protocol/internal_messages.py`
- `python/src/common/middleware/middleware_rabbitmq.py`
- `python/src/gateway/message_handler/message_handler.py`
- `python/src/sum/main.py`
- `python/src/aggregation/main.py`
- `python/src/join/main.py`

## Ejecución

En la carpeta `python`:

- `make up`
- `make logs`
- `make test`
- `make down`

### Selección de escenarios de test

En la carpeta `python`, para cambiar entre escenarios del 1 al 5:

- `make switch`

Luego ejecutar:

- `make test`

También puede hacerse sin interacción:

- `echo 3 | make switch && make test`


## Consideraciones finales

La implementación prioriza coordinación explícita, aislamiento por cliente y escalado horizontal evitando broadcast de datos. 
