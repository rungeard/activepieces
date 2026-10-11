import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import type { Store } from '@activepieces/pieces-framework';
import type { MqttAuthProps } from '../auth';
import { mqttClient } from './client';
import { mqttSessionStore } from './session-store';

for (const version of ['4', '5']) {
  const qosLevels: (1 | 2)[] = [1, 2];
  for (const qos of qosLevels) {
    test(`EMQX MQTT ${version}, QoS ${qos}: retained setup and offline messages survive bounded polls`, {
      skip: process.env.RUN_MQTT_EMQX_TESTS !== '1',
      timeout: 45_000,
    }, async () => {
      const topic = `activepieces/pr-16350/test/${randomUUID()}`;
      const clientId = `ap-test-${randomUUID().slice(0, 12)}`;
      const auth: MqttAuthProps = {
        protocol: 'mqtt', host: 'broker.emqx.io', port: 1883, mqttVersion: version,
        authMethod: 'anonymous', connectTimeoutSeconds: 10, keepAliveSeconds: 60,
        path: undefined, username: undefined, password: undefined,
        clientCertificate: undefined, clientKey: undefined, clientKeyPassphrase: undefined,
        enhancedAuthMethod: undefined, enhancedAuthData: undefined, caCertificate: undefined,
        rejectUnauthorized: true, servername: undefined, clientIdPrefix: 'ap-test',
      };
      const values = new Map<string, string>();
      const store: Store = {
        async get<T>(key: string): Promise<T | null> {
          const value = values.get(key);
          return value === undefined ? null : JSON.parse(value);
        },
        async put<T>(key: string, value: T): Promise<T> {
          values.set(key, JSON.stringify(value));
          return value;
        },
        async delete(key: string) { values.delete(key); },
      };
      const subscription = { topic, options: { qos, rh: 1 } };
      try {
        await mqttClient.publish({ auth, topic, payload: Buffer.from('retained-test'), options: { qos, retain: true } });
        await mqttClient.createPersistentSession({ auth, store, clientId, subscription });
        await mqttClient.publish({ auth, topic, payload: Buffer.from('offline-test'), options: { qos, retain: false } });
        const params: Parameters<typeof mqttClient.receiveMessages>[0] = {
          auth, store, clientId, clean: false, subscription,
          subscribe: 'when_session_missing', maxMessages: 1, listenMs: 1500, accept: () => true,
        };
        const payloads: string[] = [];
        let drained = false;
        for (let check = 0; check < 6; check += 1) {
          const received = await mqttClient.receiveMessages(params);
          assert.ok(received.length <= 1);
          payloads.push(...received.map(({ packet }) => packet.payload.toString()));
          if (received.length === 0 && new Set(payloads).size === 2) {
            drained = true;
            break;
          }
        }
        assert.deepEqual([...new Set(payloads)].sort(), ['offline-test', 'retained-test']);
        assert.ok(drained);
      } finally {
        await mqttClient.publish({ auth, topic, payload: Buffer.alloc(0), options: { qos: 1, retain: true } });
        await mqttClient.deletePersistentSession({ auth, clientId, topic });
        await mqttSessionStore.remove({ store, clientId });
      }
    });
  }
}
