import assert from 'node:assert/strict';
import { createServer, Socket } from 'node:net';
import { test } from 'node:test';
import type { Store } from '@activepieces/pieces-framework';
import type { IPublishPacket } from 'mqtt';
import type { MqttAuthProps } from '../auth';
import { mqttClient } from './client';
import { mqttPayload } from './payload';
import { mqttSessionStore } from './session-store';

test('Base64 rejects impossible lengths and malformed padding', () => {
  for (const value of ['A', 'AAAAA', 'AA=', 'AAA==', 'AAAA=', '=AA', 'AA=A', 'A===']) {
    assert.throws(() => mqttPayload.encode({ format: 'base64', value }), /not valid Base64/);
  }
});

test('Base64 keeps padded, unpadded, whitespace and URL-safe input', () => {
  for (const value of ['Zg==', 'Zg', ' Z g = =\n']) {
    assert.equal(mqttPayload.encode({ format: 'base64', value }).toString(), 'f');
  }
  assert.equal(mqttPayload.encode({ format: 'base64', value: 'Zm8' }).toString(), 'fo');
  assert.equal(mqttPayload.encode({ format: 'base64', value: '-_8=' }).toString('hex'), 'fbff');
  assert.equal(mqttPayload.encode({ format: 'base64', value: '' }).length, 0);
});

test('pending messages survive JSON storage and are bounded across checks', async () => {
  const store = memoryStore();
  const first = await mqttSessionStore.open({ store, clientId: 'pending' });
  await first.saveMessage({ packet: publishPacket({ messageId: 1 }) });
  await first.saveMessage({ packet: publishPacket({ messageId: 2 }) });
  const second = await mqttSessionStore.open({ store, clientId: 'pending' });
  const messages = await second.takeMessages({ maxMessages: 1, accept: () => true });
  assert.equal(messages.length, 1);
  assert.equal(messages[0].packet.payload.toString(), 'hello');
  assert.equal(messages[0].packet.properties?.correlationData?.toString('hex'), '00ff');
  const third = await mqttSessionStore.open({ store, clientId: 'pending' });
  assert.equal((await third.takeMessages({ maxMessages: 1, accept: () => true })).length, 1);
  assert.equal((await third.takeMessages({ maxMessages: 1, accept: () => true })).length, 0);
});

test('QoS 2 retries do not duplicate the inbox, but reused packet IDs remain valid', async () => {
  const store = memoryStore();
  const persistence = await mqttSessionStore.open({ store, clientId: 'reuse' });
  const packet = publishPacket({ messageId: 7, qos: 2 });
  await putIncoming({ persistence, packet });
  await persistence.saveMessage({ packet });
  const resumed = await mqttSessionStore.open({ store, clientId: 'reuse' });
  await resumed.saveMessage({ packet });
  await new Promise<void>((resolve, reject) => resumed.incomingStore.del(packet,
    (error) => error ? reject(error) : resolve()));
  await putIncoming({ persistence: resumed, packet });
  await resumed.saveMessage({ packet });
  assert.equal((await resumed.takeMessages({ maxMessages: 10, accept: () => true })).length, 2);
});

test('failed persistence prevents successful incoming-store acknowledgment', async () => {
  const store = memoryStore();
  store.put = async () => { throw new Error('storage unavailable'); };
  const persistence = await mqttSessionStore.open({ store, clientId: 'failure' });
  await assert.rejects(putIncoming({ persistence, packet: publishPacket({ qos: 2 }) }), /storage unavailable/);
  await assert.rejects(persistence.saveMessage({ packet: publishPacket({}) }), /storage unavailable/);
});

test('MQTT does not send PUBREC when durable storage fails', async () => {
  let pubrecCount = 0;
  await withBroker({ version: '5', onPacket: ({ socket, command }) => {
    if (command === 1) {
      sendConnack({ socket, version: '5', sessionPresent: true });
      socket.write(encodePublish({ version: '5', qos: 2, retained: false }));
    } else if (command === 5) {
      pubrecCount += 1;
    }
  } }, async ({ auth }) => {
    const store = memoryStore();
    store.put = async () => { throw new Error('storage unavailable'); };
    await assert.rejects(mqttClient.receiveMessages({
      auth, store, clientId: 'failed-write', clean: false,
      subscription: { topic: 't', options: { qos: 2 } },
      subscribe: 'when_session_missing', maxMessages: 1, listenMs: 20, accept: () => true,
    }), /storage unavailable/);
    assert.equal(pubrecCount, 0);
  });
});

for (const version of ['4', '5']) {
  test(`MQTT ${version}: delayed PUBREL survives a disconnected polling client`, async () => {
    await withBroker({ version, onPacket: ({ socket, command, connection }) => {
      if (command === 1) {
        sendConnack({ socket, version, sessionPresent: true });
        socket.write(connection === 1 ? encodePublish({ version, qos: 2, retained: false })
          : Buffer.from([0x62, 2, 0, 7]));
      }
    } }, async ({ auth }) => {
      const store = memoryStore();
      const params: Parameters<typeof mqttClient.receiveMessages>[0] = {
        auth, store, clientId: 'qos2', clean: false,
        subscription: { topic: 't', options: { qos: 2 } },
        subscribe: 'when_session_missing',
        maxMessages: 10, listenMs: 20, accept: () => true,
      };
      assert.equal((await mqttClient.receiveMessages(params)).length, 0);
      const received = await mqttClient.receiveMessages(params);
      assert.equal(received.length, 1);
      assert.equal(received[0].packet.payload.toString(), 'hello');
      assert.equal(received[0].packet.qos, 2);
    });
  });

  test(`MQTT ${version}: setup retains both live and retained messages for the first poll`, async () => {
    let subscriptionId = Buffer.alloc(0);
    let acknowledgments = 0;
    await withBroker({ version, onPacket: ({ socket, command, body, connection }) => {
      if (command === 1) {
        sendConnack({ socket, version, sessionPresent: connection > 1 });
      } else if (command === 8) {
        subscriptionId = body.subarray(0, 2);
        socket.write(encodePublish({ version, qos: 1, retained: true }));
      } else if (command === 4) {
        acknowledgments += 1;
        if (acknowledgments === 1) {
          socket.write(encodePublish({ version, qos: 1, retained: false }));
        } else {
          const response = version === '5' ? [0x90, 4, ...subscriptionId, 0, 1]
            : [0x90, 3, ...subscriptionId, 1];
          socket.write(Buffer.from(response));
        }
      }
    } }, async ({ auth }) => {
      const store = memoryStore();
      const subscription: Parameters<typeof mqttClient.createPersistentSession>[0]['subscription'] = {
        topic: 't', options: { qos: 1 },
      };
      await mqttClient.createPersistentSession({ auth, store, clientId: 'setup', subscription });
      assert.equal(acknowledgments, 2);
      const received = await mqttClient.receiveMessages({
        auth, store, clientId: 'setup', clean: false, subscription,
        subscribe: 'when_session_missing', maxMessages: 10, listenMs: 20, accept: () => true,
      });
      assert.equal(received.length, 2);
      assert.deepEqual(received.map(({ packet }) => packet.retain), [true, false]);
    });
  });
}

function memoryStore(): Store {
  const values = new Map<string, string>();
  return {
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
}

function publishPacket({ messageId = 7, qos = 1 }: { messageId?: number; qos?: 1 | 2 }): IPublishPacket {
  return { cmd: 'publish', topic: 't', payload: Buffer.from('hello'), messageId,
    qos, dup: false, retain: false, properties: { correlationData: Buffer.from([0, 255]) } };
}

async function putIncoming({ persistence, packet }: {
  persistence: Awaited<ReturnType<typeof mqttSessionStore.open>>;
  packet: IPublishPacket;
}): Promise<void> {
  await new Promise<void>((resolve, reject) => persistence.incomingStore.put(packet,
    (error) => error ? reject(error) : resolve()));
}

function sendConnack({ socket, version, sessionPresent }: {
  socket: Socket; version: string; sessionPresent: boolean;
}) {
  socket.write(Buffer.from(version === '5' ? [0x20, 3, Number(sessionPresent), 0, 0]
    : [0x20, 2, Number(sessionPresent), 0]));
}

function encodePublish({ version, qos, retained }: { version: string; qos: number; retained: boolean }) {
  const body = Buffer.from([0, 1, 116, 0, 7, ...(version === '5' ? [0] : []), ...Buffer.from('hello')]);
  return Buffer.concat([Buffer.from([0x30 | (qos << 1) | Number(retained), body.length]), body]);
}

async function withBroker({ version, onPacket }: {
  version: string;
  onPacket: (packet: { socket: Socket; command: number; body: Buffer; connection: number }) => void;
}, run: (params: { auth: MqttAuthProps }) => Promise<void>) {
  const sockets = new Set<Socket>();
  let connections = 0;
  const server = createServer((socket) => {
    connections += 1;
    const connection = connections;
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    let buffered = Buffer.alloc(0);
    socket.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 2) {
        let remaining = 0;
        let multiplier = 1;
        let position = 1;
        let byte: number;
        do {
          if (position >= buffered.length) { return; }
          byte = buffered[position++];
          remaining += (byte & 127) * multiplier;
          multiplier *= 128;
        } while (byte & 128);
        if (buffered.length < position + remaining) { return; }
        const command = buffered[0] >> 4;
        const body = buffered.subarray(position, position + remaining);
        buffered = buffered.subarray(position + remaining);
        onPacket({ socket, command, body, connection });
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const auth: MqttAuthProps = {
    protocol: 'mqtt', host: '127.0.0.1', port: address.port, mqttVersion: version,
    authMethod: 'anonymous', connectTimeoutSeconds: 2, keepAliveSeconds: 60,
    path: undefined, username: undefined, password: undefined,
    clientCertificate: undefined, clientKey: undefined, clientKeyPassphrase: undefined,
    enhancedAuthMethod: undefined, enhancedAuthData: undefined, caCertificate: undefined,
    rejectUnauthorized: true, servername: undefined, clientIdPrefix: 'test',
  };
  try {
    await run({ auth });
  } finally {
    for (const socket of sockets) { socket.destroy(); }
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}
