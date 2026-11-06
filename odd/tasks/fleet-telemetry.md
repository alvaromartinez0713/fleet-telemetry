# fleet-telemetry — Portal Corporativo y Telemetría de Flotas

MVP de portal corporativo para monitoreo de flotas: pipeline de ingesta de telemetría de alta concurrencia, agente de IA y app móvil offline-first. Prueba técnica "Portal Corporativo y Telemetría" adoptada como proyecto de aprendizaje (sin deadline, se cumple todo lo pedido).

## Specs

Fuente: enunciado verbatim de la prueba (Engram obs #40).

- **S1** Ingesta: "Implementar un flujo de ingesta asíncrona usando un bus de eventos (ej. Kafka, RabbitMQ) para procesamiento desacoplado."
- **S2** Persistencia: "Diseñar y justificar la capa de almacenamiento pensando en miles de dispositivos (ej. bases de datos de series de tiempo como TimescaleDB)."
- **S3** Resiliencia: "Implementar Circuit Breakers en la comunicación entre microservicios."
- **S4** Agente: "Agente Operativo: Implementar un agente (LangChain, Semantic Kernel, etc.) integrado en tu backend que consuma el estado actual de los vehículos y responda consultas en lenguaje natural (ej. '¿Qué vehículos llevan detenidos más de 20 minutos en zonas críticas?')."
- **S5** Portal: "Dashboard Reactivo: SPA que consuma los datos procesados mediante WebSockets o SSE (Server-Sent Events). Mostrar mapa, alertas en tiempo real y chat con la IA."
- **S6** Móvil: "App móvil (React Native u otro) que envíe coordenadas. Debe persistir localmente (SQLite, WatermelonDB) si se pierde la red y sincronizar en bloque al reconectar."
- **S7** CI/CD móvil: "Incluir archivos de automatización de despliegue (Fastlane, GitHub Actions)."
- **S8** Caos y carga: "Script (k6, JMeter) para simular cientos de vehículos, inyectando 10% de peticiones duplicadas y 5% de errores."
- **S9** Infra: "Proveer scripts de IaC (Terraform, AWS CDK) y ejecución local orquestada vía Docker Compose."
- **S10** README: "Instrucciones de ejecución e IaC."
- **S11** Repo: "Con historial de commits estructurado."

Condiciones del usuario: sin deadline; sustentación no aplica; README según lo pedido; agente con function calling directo sin LangChain.

Decisiones de arquitectura cerradas (Engram obs #41 rev 10, #10): monorepo con 5 binarios Go (INGEST, CONSUMER, ALERTS, AGENT, GATEWAY); Redpanda local; TimescaleDB con `UNIQUE(device_id, seq)` como dedup permanente; watermark cumulativo + FIFO para borrado del SQLite de la app; `device_id` = identidad de instalación (reinstalar = nuevo registro = watermark 0), `driver_id` = identidad de negocio; JWT ~90d stateless; micro-batches siempre (flush 2-5s); traceparent W3C desde el dispositivo; 3 estados de vehículo (movimiento / detenido con heartbeat ~60s / sin señal) con frecuencia adaptativa; CB solo en gateway→agent y agent→LLM; consumers publican a tiempo real vía Redis pub/sub para SSE; agente Go con loop de function calling directo, 8-10 tools incl. `salud_pipeline()`; evals del agente en Python offline. Detalle y justificación de cada decisión: `docs/decisions.md`.

## Tasks

- **T1** (S11) docs: feature doc ODD + ADRs (`docs/decisions.md`) + diagrama de arquitectura (`docs/architecture.excalidraw`) — commit `1d1ebc3`, revisado en `0b0b635`
- **T2** (S9) esqueleto del monorepo: estructura Go, 5 `cmd/`, Docker Compose con Redpanda + TimescaleDB — commit `903f80e`
- **T3** (S1, S2) slice 1: contracts + migración Timescale + INGEST (`POST /v1/telemetry`, JWT estático de dev) + CONSUMER (dedup `UNIQUE(device_id, captured_at, seq)`, tres timestamps, watermark transaccional) — pendiente
- **T4** (S1) GATEWAY REST (`GET /v1/devices/self/watermark`) + SSE con Redis pub/sub — pendiente
- **T5** (S5) portal web reactivo (mapa, alertas, chat) — pendiente
- **T6** (S3, S4) agente IA + circuit breakers (gateway→agent, agent→LLM) + tools — pendiente
- **T7** (S4) ALERTS (detenido >20 min en zona crítica) — pendiente
- **T8** (S6) app móvil offline-first (SQLite, cola append-only, sync por lotes) + `POST /v1/devices/register` con JWT real — pendiente
- **T9** (S7) CI/CD móvil (EAS/Fastlane + GitHub Actions) — pendiente
- **T10** (S8) k6: cientos de vehículos, 10% duplicados, 5% errores — pendiente
- **T11** (S9, S10) IaC Terraform documentado + README final — pendiente

## Log

- **L1** (pedido original, verbatim): "aca vamos a iniciar el desarrollo de fleet-telemetry busca en engram todo lo que ya tenemos definido para este proyecto y vamos a definir primero con que vamos a iniciar"
- **L2**: usuario eligió "Docs + esqueleto": feature doc + ADRs + esqueleto del monorepo con Docker Compose; el slice funcional (T3) queda para la próxima sesión.
- **L3** (verbatim): "hay algunos detalles yo se que hay decisiones que se tomaron y estan guardadas asi deben quedar en engram, pero los ADR en decision no tienen mucho sentido y no cumplen realmente todas son decisiones importantes de arquiterura o para el comportamiento de la aplicacion, el archivo ADR me gusta como tiene el indice pero mira la estructura de como se crearon en otro proyecto el de urlshortener, esa ruta de ingest no me gusta el nombre deberia ser algo como /v1/telemetry sin ese batch, antes de iniciar a crear codigo me gustaria que hicieras un diseño en excaldraw de como esta conectada toda la app como va funcionar que se conecta con que y como"
  Consecuencias: `docs/decisions.md` reescrito con la estructura de urlshortener (5 ADRs consolidados con Contexto / Decisiones / Consecuencias / Referencias); ruta de ingesta es `POST /v1/telemetry` (sin `/batch`); diagrama de arquitectura en `docs/architecture.excalidraw` + preview PNG antes de escribir código.
- **L4**: nota de alcance de revisión — `docs/architecture.excalidraw` (3057 líneas en el diff) es JSON generado por la herramienta de diagramas, no trabajo de autor; el diff revisable de ese slice es `docs/decisions.md` + este documento (~540 líneas). No se parte el slice por el artefacto generado.
- **L5** (decisiones tomadas antes de T3, respuestas del usuario):
  - Dedup y timestamps (respuesta custom, verbatim): "pero en el dispositivo se deberia guardar la fecha exacta en la que captura el evento y tambien se deberia almacenar, y en el ingesta tambien se deberia tomar la fecha en la que se recibe el evento, y en el cosumer tambien guardar un store_at en la que se guarda el evento en la base datos" → `captured_at` (dispositivo, inmutable) / `received_at` (ingesta) / `stored_at` (base), y el índice único es `UNIQUE(device_id, captured_at, seq)` porque la hyptable exige la columna de partición.
  - Endpoint de watermark: "Diferirlo a T4" → T3 prueba el algoritmo y la transacción; el endpoint nace con el GATEWAY en T4.
  - Auth: "JWT con token estático de dev" → T3 valida JWT HS256 con `sub` = device_id; el registro real va a T8.
  - Entrega del slice de docs: "Push + PR + merge a main" → PR #4 mergeada, `main` en `6b410a2`.
