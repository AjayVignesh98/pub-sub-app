import express from 'express';
import cors from 'cors';
import { v4 as uuidv4 } from 'uuid';
import PubSub from './pubsub.js';

const app = express();
app.use(cors());
app.use(express.json());

const pubsub = new PubSub();

app.get('/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });
  res.write('\n');

  const clientId = uuidv4();
  pubsub.addClient(clientId, res);

  res.write(`event: connected\ndata: ${JSON.stringify({ clientId })}\n\n`);

  const topics = pubsub.getTopics();
  if (topics.length) {
    res.write(`event: topics\ndata: ${JSON.stringify({ topics })}\n\n`);
  }

  req.on('close', () => {
    pubsub.removeClient(clientId);
  });
});

app.post('/api/subscribe', (req, res) => {
  const { clientId, topic } = req.body;
  if (!clientId || !topic) {
    return res.status(400).json({ error: 'clientId and topic required' });
  }
  pubsub.subscribe(clientId, topic);
  const topics = pubsub.getTopics();
  broadcast({ type: 'topics', topics });
  res.json({ ok: true, topic });
});

app.post('/api/unsubscribe', (req, res) => {
  const { clientId, topic } = req.body;
  if (!clientId || !topic) {
    return res.status(400).json({ error: 'clientId and topic required' });
  }
  pubsub.unsubscribe(clientId, topic);
  const topics = pubsub.getTopics();
  broadcast({ type: 'topics', topics });
  res.json({ ok: true, topic });
});

app.post('/api/publish', (req, res) => {
  const { topic, title, body } = req.body;
  if (!topic || !title) {
    return res.status(400).json({ error: 'topic and title required' });
  }
  const result = pubsub.publish(topic, { title, body: body || '' });
  const topics = pubsub.getTopics();
  broadcast({ type: 'topics', topics });
  res.json(result);
});

app.get('/api/topics', (req, res) => {
  res.json({ topics: pubsub.getTopics() });
});

function broadcast(data) {
  for (const [, res] of pubsub.clients) {
    try {
      res.write(`event: ${data.type}\ndata: ${JSON.stringify(data)}\n\n`);
    } catch {}
  }
}

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`\n  Pub-Sub Server running at http://localhost:${PORT}`);
  console.log(`  SSE endpoint at http://localhost:${PORT}/events\n`);
});
