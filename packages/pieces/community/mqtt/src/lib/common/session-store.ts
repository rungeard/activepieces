import type { Store as FlowStore } from '@activepieces/pieces-framework';
import { IPublishPacket, Store as MqttStore } from 'mqtt';
import type { ReceivedMessage } from './client';

async function open({ store, clientId }: { store: FlowStore; clientId: string }) {
  const key = storageKey({ clientId });
  let state = await store.get<SessionState>(key) ?? { incoming: {}, pending: [], delivered: [] };
  let writes = Promise.resolve();
  let failure: Error | undefined;
  let onError: ((error: Error) => void) | undefined;

  function update({ change }: { change: (current: SessionState) => SessionState }): Promise<void> {
    const write = writes.then(async () => {
      if (failure) {
        throw failure;
      }
      const next = change(state);
      await store.put(key, next);
      state = next;
    });
    writes = write.catch((error: unknown) => {
      failure = toError({ error });
    });
    return write;
  }

  const incomingStore = new MqttStore({ clean: false });
  const putPacket = incomingStore.put.bind(incomingStore);
  const getPacket = incomingStore.get.bind(incomingStore);
  const deletePacket = incomingStore.del.bind(incomingStore);
  for (const packet of Object.values(state.incoming)) {
    putPacket(deserializePacket({ packet }), () => undefined);
  }
  incomingStore.put = (packet, callback) => {
    if (packet.cmd !== 'publish' || packet.messageId === undefined) {
      callback(new Error('Expected an incoming MQTT publish packet with a message ID.'));
      return incomingStore;
    }
    const messageId = packet.messageId;
    const serialized = serializePacket({ packet });
    void update({ change: (current) => ({
      ...current,
      incoming: { ...current.incoming, [messageId]: serialized },
      delivered: packet.dup && current.incoming[messageId] ? current.delivered
        : current.delivered.filter((id) => id !== messageId),
    }) }).then(() => putPacket(packet, callback), (error: unknown) => {
      if (onError) {
        onError(toError({ error }));
      } else {
        callback(toError({ error }));
      }
    });
    return incomingStore;
  };
  incomingStore.get = (packet, callback) => {
    void writes.then(() => {
      if (failure) {
        callback(failure);
      } else {
        getPacket(packet, callback);
      }
    });
    return incomingStore;
  };
  incomingStore.del = (packet, callback) => {
    void update({ change: (current) => {
      const incoming = Object.fromEntries(Object.entries(current.incoming)
        .filter(([id]) => id !== String(packet.messageId)));
      return { ...current, incoming, delivered: current.delivered.filter((id) => id !== packet.messageId) };
    } }).then(() => deletePacket(packet, callback),
      (error: unknown) => callback(toError({ error })));
    return incomingStore;
  };
  incomingStore.close = (callback) => {
    void writes.then(() => callback(failure));
  };

  async function saveMessage({ packet }: { packet: IPublishPacket }): Promise<void> {
    const message: StoredMessage = {
      packet: serializePacket({ packet }),
      receivedAt: new Date().toISOString(),
    };
    await update({ change: (current) => {
      const messageId = packet.messageId;
      const alreadyPending = packet.qos === 2 && messageId !== undefined && current.delivered.includes(messageId);
      return alreadyPending ? current : {
        ...current,
        pending: [...current.pending, message],
        delivered: packet.qos === 2 && messageId !== undefined
          ? [...current.delivered, messageId] : current.delivered,
      };
    } });
  }

  async function takeMessages({
    maxMessages,
    accept,
  }: {
    maxMessages: number;
    accept: (packet: IPublishPacket) => boolean;
  }): Promise<ReceivedMessage[]> {
    const received: ReceivedMessage[] = [];
    await update({ change: (current) => {
      const pending = current.pending.filter((message) => {
        const packet = deserializePacket({ packet: message.packet });
        if (!accept(packet)) {
          return false;
        }
        if (received.length >= maxMessages) {
          return true;
        }
        received.push({ topic: packet.topic, packet, receivedAt: message.receivedAt });
        return false;
      });
      return { ...current, pending };
    } });
    return received;
  }

  function setErrorHandler({ handler }: { handler: (error: Error) => void }): void {
    onError = handler;
  }

  return { incomingStore, saveMessage, takeMessages, setErrorHandler };
}

async function remove({ store, clientId }: { store: FlowStore; clientId: string }): Promise<void> {
  await store.delete(storageKey({ clientId }));
}

function storageKey({ clientId }: { clientId: string }): string {
  return `mqtt_session_messages:${clientId}`;
}

function serializePacket({ packet }: { packet: IPublishPacket }): StoredPacket {
  const payload = typeof packet.payload === 'string' ? Buffer.from(packet.payload) : packet.payload;
  return {
    ...packet,
    payload: payload.toString('base64'),
    properties: packet.properties ? {
      ...packet.properties,
      correlationData: packet.properties.correlationData?.toString('base64'),
    } : undefined,
  };
}

function deserializePacket({ packet }: { packet: StoredPacket }): IPublishPacket {
  return {
    ...packet,
    payload: Buffer.from(packet.payload, 'base64'),
    properties: packet.properties ? {
      ...packet.properties,
      correlationData: packet.properties.correlationData === undefined ? undefined
        : Buffer.from(packet.properties.correlationData, 'base64'),
    } : undefined,
  };
}

function toError({ error }: { error: unknown }): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export const mqttSessionStore = { open, remove };

export type SessionPersistence = Awaited<ReturnType<typeof open>>;

type StoredPacket = Omit<IPublishPacket, 'payload' | 'properties'> & {
  payload: string;
  properties?: Omit<NonNullable<IPublishPacket['properties']>, 'correlationData'> & {
    correlationData?: string;
  };
};

type StoredMessage = { packet: StoredPacket; receivedAt: string };
type SessionState = { incoming: Record<string, StoredPacket>; pending: StoredMessage[]; delivered: number[] };
