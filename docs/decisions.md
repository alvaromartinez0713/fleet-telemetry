# Decisiones de arquitectura (ADRs lean)

Índice de decisiones con su porqué. Formato: contexto → decisión → porqué. Solo decisiones que lo ameritan; el detalle operativo vive en el código y el feature doc (`odd/tasks/fleet-telemetry.md`).

## Índice

1. [Monorepo con 5 binarios Go](#adr-001-monorepo-con-5-binarios-go)
2. [TimescaleDB sobre Cassandra](#adr-002-timescaledb-sobre-cassandra)
3. [Redpanda como bus de eventos local](#adr-003-redpanda-como-bus-de-eventos-local)
4. [Dedup permanente con UNIQUE(device_id, seq)](#adr-004-dedup-permanente-con-unicuedevice_id-seq)
5. [Watermark cumulativo + FIFO para borrado en la app](#adr-005-watermark-cumulativo--fifo-para-borrado-en-la-app)
6. [device_id = identidad de instalación; driver_id = identidad de negocio](#adr-006-device_id--identidad-de-instalación-driver_id--identidad-de-negocio)
7. [JWT ~90d stateless](#adr-007-jwt-90d-stateless)
8. [Micro-batches siempre (flush 2-5s)](#adr-008-micro-batches-siempre-flush-2-5s)
9. [SSE sobre WebSockets](#adr-009-sse-sobre-websockets)
10. [Redis pub/sub como fan-out a tiempo real](#adr-010-redis-pubsub-como-fan-out-a-tiempo-real)
11. [3 estados de vehículo + frecuencia adaptativa](#adr-011-3-estados-de-vehículo--frecuencia-adaptativa)
12. [Nunca descartar eventos por valor](#adr-012-nunca-descartar-eventos-por-valor)
13. [Circuit breakers solo en gateway→agent y agent→LLM](#adr-013-circuit-breakers-solo-en-gatewayagent-y-agentllm)
14. [Agente Go con function calling directo, sin LangChain](#adr-014-agente-go-con-function-calling-directo-sin-langchain)
15. [traceparent W3C desde el dispositivo](#adr-015-traceparent-w3c-desde-el-dispositivo)
16. [IaC Terraform documentado, ejecución local vía Docker Compose](#adr-016-iac-terraform-documentado-ejecución-local-vía-docker-compose)

## ADR-001: Monorepo con 5 binarios Go

Un solo repositorio con cinco binarios: **INGEST** (recibe eventos HTTP), **CONSUMER** (consume del bus, deduplica, persiste), **ALERTS** (reglas sobre estado), **AGENT** (LLM + tools), **GATEWAY** (API REST + SSE).

Porqué: la prueba evalúa arquitectura, no despliegue de microservicios reales; el monorepo mantiene contratos y tooling unificados mientras cada binario es desplegable por separado. Los servicios comunican solo vía bus de eventos (desacoplados), no RPC directo.

## ADR-002: TimescaleDB sobre Cassandra

Persistencia de series de tiempo en TimescaleDB (extensión de Postgres), no Cassandra.

Porqué: hypertables, compresión columnar nativa por chunk, continuous aggregates y retención por política cubren el volumen con una sola tecnología que ya conocemos (SQL). Cassandra sería sobre-operación para este volumen y agrega costo operativo (operación + modelado query-first) sin beneficio. La justificación es parte del entregable (S2).

## ADR-003: Redpanda como bus de eventos local

Kafka en producción conceptual; Redpanda para ejecución local (Docker Compose).

Porqué: Kafka-compatible API, un binario, sin Zookeeper, apto para hardware local. El código Go usa el protocolo Kafka estándar (franz-go o similar), así la elección local no contamina el diseño.

## ADR-004: Dedup permanente con UNIQUE(device_id, seq)

Kafka entrega at-least-once: el consumidor puede recibir el mismo evento más de una vez. Cada evento lleva `seq` monótono por dispositivo; la hypertable tiene `UNIQUE(device_id, seq)` y el insert usa `ON CONFLICT DO NOTHING`.

Porqué: dedup idempotente sin estado adicional, sin tablas de event_id, sin TTL de dedup. El `seq` también ordena eventos por dispositivo.

## ADR-005: Watermark cumulativo + FIFO para borrado en la app

La app móvil borra del SQLite local solo eventos con `seq ≤ watermark`, donde el watermark es el mínimo cumulativo confirmado por el servidor (FIFO), **nunca** "máx seq recibida".

Porqué: con out-of-order (reintentos, batch parcial), un watermark = máx seq borraría eventos que el servidor no recibió → pérdida silenciosa de datos. El watermark cumulativo garantiza que solo se borra lo efectivamente persistido. Corrección detectada y acordada en diseño (el hueco fue encontrado en revisión conjunta).

## ADR-006: device_id = identidad de instalación; driver_id = identidad de negocio

`device_id` se genera por instalación de la app y se registra en `/devices/register`; reinstalar la app = nuevo device_id = watermark 0. `driver_id` es la identidad de negocio del conductor, vinculada al device en el registro.

Porqué: separa identidad de hardware/instalación de identidad humana; evita heredar state corrupto tras reinstalación y simplifica el contrato del watermark.

## ADR-007: JWT ~90d stateless

Autenticación de dispositivos con JWT de vida larga (~90 días), sin estado en el servidor.

Porqué: miles de dispositivos emitiendo cada 2-5s no pueden pagar un round-trip de auth por request ni un store de sesiones; el refresh/reemplazo ocurre en el registro.

## ADR-008: Micro-batches siempre (flush 2-5s)

INGEST y CONSUMER trabajan siempre con micro-batches: acumulan y hacen flush cada 2-5s o N eventos, lo que ocurra primero. No existe modo "individual" alternativo.

Porqué: una sola code path, mejor throughput en inserts a Timescale, presión de red razonable. La dualidad individual/lote agrega ramas sin valor a este volumen.

## ADR-009: SSE sobre WebSockets

El portal consume datos en tiempo real vía SSE (Server-Sent Events), no WebSockets.

Porqué: el flujo es unidireccional servidor→cliente; SSE es HTTP puro (proxy-friendly, reconexión nativa del navegador, `proxy_buffering off` en nginx/Caddy) y alcanza exactamente para mapa + alertas + feed.

## ADR-010: Redis pub/sub como fan-out a tiempo real

Los CONSUMERS publican cada evento procesado a Redis pub/sub; el GATEWAY suscribe y lo reenvía por SSE a los dashboards.

Porqué: N consumers escalables por particiones y N gateways sin acoplamiento directo; el pub/sub de Redis es el bus efímero natural para "lo último", mientras Timescale es el bus durable para "todo el historial".

## ADR-011: 3 estados de vehículo + frecuencia adaptativa

El dispositivo reporta en dos modos: en **movimiento**, evento cada 2-5s (~720-1800/h); **detenido**, heartbeat cada ~60s (~60/h) — reducción ~30x. El sistema deriva el tercer estado, **sin señal**, cuando no llega nada dentro de una ventana esperada. El corazón del modelo: "detenido vivo" ≠ "sin señal" se distingue por el heartbeat.

Porqué: el volumen total lo domina lo móvil; el heartbeat es el precio mínimo de la señal de vida y protege batería. KISS: sin lógica de delta de posición en la app.

## ADR-012: Nunca descartar eventos por valor

El CONSUMER no descarta eventos por contenido repetido (misma posición, mismo valor). Todo evento enviado se persiste; la compresión nativa de Timescale hace marginal el costo del dato repetido.

Porqué: el contrato del watermark promete "todo lo enviado queda persistido"; si el consumer salteara por valor, el watermark no podría avanzar y la app no borraría → contrato roto. Reducción de volumen = responsabilidad de la app (frecuencia adaptativa), no del consumer.

## ADR-013: Circuit breakers solo en gateway→agent y agent→LLM

Circuit breakers (gobreaker) únicamente en las comunicaciones síncronas: GATEWAY→AGENT y AGENT→LLM.

Porqué: la vía de eventos (ingest→Kafka→consumer→persistencia) ya es asíncrona con backpressure natural; un CB ahí no aporta. El CB protege donde un colapso sincrónico degradaría al usuario: el chat IA. Cubre S3.

## ADR-014: Agente Go con function calling directo, sin LangChain

El AGENT implementa el loop de function calling directamente en Go contra la API del LLM. Catálogo de 8-10 tools limpias, incluida `salud_pipeline()` (salud del pipeline como dato consultable). Evals en Python, offline, sobre grabaciones.

Porqué: sin framework el flujo es auditable (evalúa Clean Architecture), con menos dependencias y menos magia; YAGNI. Los evals separados del runtime permiten medir calidad sin acoplar Python al binario.

## ADR-015: traceparent W3C desde el dispositivo

Cada evento generado por la app incluye header/contexto `traceparent` W3C, propagado a través de todo el pipeline.

Porqué: trazabilidad end-to-end (dispositivo → ingest → consumer → persistencia) con el estándar; `trace_id` compartido por todos los logs del recorrido de un evento.

## ADR-016: IaC Terraform documentado, ejecución local vía Docker Compose

Se entregan scripts Terraform (AWS) documentados (no desplegados) y la ejecución local real se orquesta con Docker Compose. Si el hardware local no soporta todo el stack simultáneo, se justifican mocks ligeros o free tiers.

Porqué: el enunciado exige IaC + Docker Compose (S9) y acepta explícitamente este criterio ejecutivo; Terraform documentado demuestra la decisión de infraestructura sin costo de nube.
