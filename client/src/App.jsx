import { useState, useEffect, useRef, useCallback } from 'react';
import './App.css';

const API = 'http://localhost:3002';

export default function App() {
  const [clientId, setClientId] = useState(null);
  const [connected, setConnected] = useState(false);
  const [topics, setTopics] = useState([]);
  const [subscriptions, setSubscriptions] = useState([]);
  const [messages, setMessages] = useState([]);
  const [publishTopic, setPublishTopic] = useState('');
  const [publishTitle, setPublishTitle] = useState('');
  const [publishBody, setPublishBody] = useState('');
  const [newTopic, setNewTopic] = useState('');
  const msgEndRef = useRef(null);

  useEffect(() => {
    const es = new EventSource(`${API}/events`);

    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);

    es.addEventListener('connected', (e) => {
      const { clientId } = JSON.parse(e.data);
      setClientId(clientId);
    });

    es.addEventListener('topics', (e) => {
      const { topics } = JSON.parse(e.data);
      setTopics(topics);
    });

    es.addEventListener('subscribed', (e) => {
      const { topic } = JSON.parse(e.data);
      setSubscriptions((prev) => (prev.includes(topic) ? prev : [...prev, topic]));
    });

    es.addEventListener('unsubscribed', (e) => {
      const { topic } = JSON.parse(e.data);
      setSubscriptions((prev) => prev.filter((t) => t !== topic));
    });

    es.addEventListener('message', (e) => {
      const msg = JSON.parse(e.data);
      setMessages((prev) => [...prev, msg]);
    });

    return () => es.close();
  }, []);

  useEffect(() => {
    msgEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const subscribe = useCallback(
    async (topic) => {
      if (!clientId) return;
      await fetch(`${API}/api/subscribe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clientId, topic }),
      });
    },
    [clientId]
  );

  const unsubscribe = useCallback(
    async (topic) => {
      if (!clientId) return;
      await fetch(`${API}/api/unsubscribe`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clientId, topic }),
      });
    },
    [clientId]
  );

  const publish = async (e) => {
    e.preventDefault();
    const topic = publishTopic || newTopic;
    if (!topic || !publishTitle) return;
    await fetch(`${API}/api/publish`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic, title: publishTitle, body: publishBody }),
    });
    setPublishTitle('');
    setPublishBody('');
    if (!subscriptions.includes(topic)) {
      setPublishTopic('');
      setNewTopic('');
    }
  };

  const existingTopics = topics.map((t) => t.topic);
  const allTopics = [...new Set([...existingTopics, ...subscriptions])];

  return (
    <div className="app">
      <header>
        <h1>Pub-Sub Playground</h1>
        <p className="subtitle">
          Publish messages to topics — subscribers receive them in real time
        </p>
        <span className={`badge ${connected ? 'on' : 'off'}`}>
          {connected ? '● Connected' : '○ Disconnected'}
        </span>
        {clientId && (
          <span className="client-id">ID: {clientId.slice(0, 8)}</span>
        )}
      </header>

      <div className="layout">
        <section className="card pub-card">
          <h2>📢 Publish</h2>
          <form onSubmit={publish}>
            <label>
              Topic
              <div className="topic-row">
                <select
                  value={publishTopic}
                  onChange={(e) => {
                    setPublishTopic(e.target.value);
                    setNewTopic('');
                  }}
                >
                  <option value="">— Select or type new —</option>
                  {existingTopics.map((t) => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
                <input
                  type="text"
                  placeholder="New topic"
                  value={newTopic}
                  onChange={(e) => {
                    setNewTopic(e.target.value);
                    setPublishTopic('');
                  }}
                />
              </div>
            </label>
            <label>
              Title
              <input
                type="text"
                placeholder="Message title"
                value={publishTitle}
                onChange={(e) => setPublishTitle(e.target.value)}
                required
              />
            </label>
            <label>
              Body (optional)
              <textarea
                placeholder="Message body"
                value={publishBody}
                onChange={(e) => setPublishBody(e.target.value)}
                rows={2}
              />
            </label>
            <button type="submit">Publish</button>
          </form>
        </section>

        <section className="card sub-card">
          <h2>🔔 Subscriptions</h2>
          {allTopics.length === 0 && (
            <p className="hint">
              No topics yet. Publish a message or type a topic above.
            </p>
          )}
          <div className="topic-list">
            {allTopics.map((topic) => {
              const info = topics.find((t) => t.topic === topic);
              const isSubbed = subscriptions.includes(topic);
              return (
                <div key={topic} className={`topic-row-item ${isSubbed ? 'subbed' : ''}`}>
                  <div className="topic-info">
                    <strong>{topic}</strong>
                    {info && (
                      <span className="topic-stats">
                        {info.subscriberCount} sub{info.subscriberCount !== 1 ? 's' : ''}
                        {' · '}
                        {info.messageCount} msg
                      </span>
                    )}
                  </div>
                  <button
                    className={`btn-${isSubbed ? 'unsub' : 'sub'}`}
                    onClick={() => (isSubbed ? unsubscribe(topic) : subscribe(topic))}
                    disabled={!clientId}
                  >
                    {isSubbed ? 'Unsubscribe' : 'Subscribe'}
                  </button>
                </div>
              );
            })}
          </div>
        </section>

        <section className="card msg-card">
          <h2>💬 Messages</h2>
          <div className="msg-count">
            {messages.length} received
          </div>
          <div className="msg-list">
            {messages.length === 0 && (
              <p className="hint">
                Subscribe to a topic and wait for messages...
              </p>
            )}
            {[...messages].reverse().map((msg) => (
              <div key={msg.id} className={`msg ${subscriptions.includes(msg.topic) ? '' : 'muted'}`}>
                <div className="msg-head">
                  <span className="msg-topic">{msg.topic}</span>
                  <span className="msg-time">
                    {new Date(msg.timestamp).toLocaleTimeString()}
                  </span>
                </div>
                <div className="msg-title">{msg.title}</div>
                {msg.body && <div className="msg-body">{msg.body}</div>}
              </div>
            ))}
            <div ref={msgEndRef} />
          </div>
        </section>
      </div>
    </div>
  );
}
