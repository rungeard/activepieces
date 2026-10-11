# MQTT regression tests

From the repository root, with dependencies installed and Node.js 22 or newer:

```bash
node --import tsx --test packages/pieces/community/mqtt/src/lib/common/regression.test.ts
```

The tests use a local scripted MQTT broker to interrupt a QoS 2 exchange after PUBREC, reconnect with a new client, and deliver PUBREL without resending the payload. They also exercise setup-time retained and live messages, storage failures, binary properties, packet-ID reuse, bounded delivery and Base64 validation.

To additionally test MQTT 3.1.1 and MQTT 5 with QoS 1 and 2 on the public EMQX broker:

```bash
RUN_MQTT_EMQX_TESTS=1 node --import tsx --test packages/pieces/community/mqtt/src/lib/common/emqx.test.ts
```

These opt-in tests publish fictitious payloads on unique topics at `broker.emqx.io:1883`, verify retained and offline delivery across polling connections, and remove their retained messages and broker sessions afterwards. QoS 1 redelivery is allowed. External broker availability can affect these tests.

Incoming QoS 2 packets are persisted before PUBREC. Received messages are saved to a durable inbox before PUBACK or PUBCOMP and collected by subsequent polls, including messages received during setup. The poll limit bounds returned messages; already accepted messages beyond that limit stay in the inbox. MQTT acknowledgments do not guarantee exactly-once execution of an Activepieces flow.
