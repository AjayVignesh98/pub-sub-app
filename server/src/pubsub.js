import { v4 as uuidv4 } from 'uuid';

class PubSub {
  constructor() {
    this.subscriptions = new Map();
    this.clients = new Map();
    this.topicMessages = new Map();
    this.brokerLog = [];
  }

  addClient(id, res) {
    this.clients.set(id, res);
    this.brokerLog.push({
      type: 'client.connect',
      clientId: id,
      timestamp: new Date().toISOString(),
    });
  }

  removeClient(id) {
    this.clients.delete(id);
    for (const [, subs] of this.subscriptions) {
      subs.delete(id);
    }
    this.brokerLog.push({
      type: 'client.disconnect',
      clientId: id,
      timestamp: new Date().toISOString(),
    });
  }

  subscribe(clientId, topic) {
    if (!this.subscriptions.has(topic)) {
      this.subscriptions.set(topic, new Set());
      this.topicMessages.set(topic, []);
    }
    this.subscriptions.get(topic).add(clientId);

    const res = this.clients.get(clientId);
    if (res) {
      res.write(`event: subscribed\ndata: ${JSON.stringify({ topic })}\n\n`);
    }

    this.brokerLog.push({
      type: 'subscribe',
      clientId,
      topic,
      timestamp: new Date().toISOString(),
    });
  }

  unsubscribe(clientId, topic) {
    const subs = this.subscriptions.get(topic);
    if (subs) {
      subs.delete(clientId);
      const res = this.clients.get(clientId);
      if (res) {
        res.write(`event: unsubscribed\ndata: ${JSON.stringify({ topic })}\n\n`);
      }
    }
    this.brokerLog.push({
      type: 'unsubscribe',
      clientId,
      topic,
      timestamp: new Date().toISOString(),
    });
  }

  publish(topic, payload) {
    const msg = {
      id: uuidv4().slice(0, 8),
      topic,
      ...payload,
      timestamp: new Date().toISOString(),
    };

    if (!this.subscriptions.has(topic)) {
      this.subscriptions.set(topic, new Set());
    }
    if (!this.topicMessages.has(topic)) {
      this.topicMessages.set(topic, []);
    }
    this.topicMessages.get(topic).push(msg);
    if (this.topicMessages.get(topic).length > 100) {
      this.topicMessages.get(topic).shift();
    }

    const subscriberIds = this.subscriptions.get(topic);
    let delivered = 0;
    if (subscriberIds) {
      for (const clientId of subscriberIds) {
        const res = this.clients.get(clientId);
        if (res) {
          res.write(`event: message\ndata: ${JSON.stringify(msg)}\n\n`);
          delivered++;
        }
      }
    }

    this.brokerLog.push({
      type: 'publish',
      topic,
      msgId: msg.id,
      subscriberCount: subscriberIds?.size || 0,
      delivered,
      timestamp: new Date().toISOString(),
    });

    return { msg, delivered, subscriberCount: subscriberIds?.size || 0 };
  }

  getTopics() {
    return Array.from(this.subscriptions.entries()).map(([topic, subs]) => ({
      topic,
      subscriberCount: subs.size,
      messageCount: (this.topicMessages.get(topic) || []).length,
    }));
  }

  getBrokerLog() {
    return this.brokerLog.slice(-100);
  }

  getClientSubscriptions(clientId) {
    const result = [];
    for (const [topic, subs] of this.subscriptions) {
      if (subs.has(clientId)) result.push(topic);
    }
    return result;
  }
}

export default PubSub;
