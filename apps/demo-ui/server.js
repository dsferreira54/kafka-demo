const express = require('express');
const path = require('path');
const https = require('https');
const fs = require('fs');
const { Pool } = require('pg');
const { Kafka } = require('kafkajs');

let oracledb;
try { oracledb = require('oracledb'); } catch (_) { /* thin mode fallback */ }

const app = express();
const PORT = process.env.PORT || 8080;

const PRODUCER_URL  = process.env.PRODUCER_URL  || 'http://kafka-producer.kafka-demo.svc:8080';
const CONSUMER_URL  = process.env.CONSUMER_URL  || 'http://kafka-consumer.kafka-demo.svc:8080';
const CONNECT_URL   = process.env.KAFKA_CONNECT_URL || 'http://kafka-connect-connect-api.kafka-demo.svc:8083';
const APICURIO_URL  = process.env.APICURIO_URL  || 'http://apicurio-registry-app-service.kafka-demo.svc:8080';
const KAFKA_BROKERS = (process.env.KAFKA_BOOTSTRAP || 'kafka-cluster-kafka-bootstrap.kafka-demo.svc:9092').split(',');

const pgPool = new Pool({
  host:     process.env.PG_HOST     || 'postgresql.kafka-demo.svc',
  port:     parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DATABASE || 'demodb',
  user:     process.env.PG_USER     || 'demouser',
  password: process.env.PG_PASSWORD || 'demo-password-123',
  max: 5,
});

const kafka = new Kafka({ clientId: 'demo-ui', brokers: KAFKA_BROKERS });

const ORA_CFG = {
  user:          process.env.ORACLE_USER     || 'debezium',
  password:      process.env.ORACLE_PASSWORD || 'OracleDemo123',
  connectString: process.env.ORACLE_CONNSTR  || 'oracle.kafka-demo.svc:1521/FREEPDB1',
};

let k8sCa, k8sToken, k8sNs;
try {
  k8sCa    = fs.readFileSync('/var/run/secrets/kubernetes.io/serviceaccount/ca.crt');
  k8sToken = fs.readFileSync('/var/run/secrets/kubernetes.io/serviceaccount/token', 'utf8').trim();
  k8sNs    = fs.readFileSync('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'utf8').trim();
} catch (_) { /* not running in cluster */ }

function k8sApi(method, apiPath) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: process.env.KUBERNETES_SERVICE_HOST,
      port: process.env.KUBERNETES_SERVICE_PORT || 443,
      path: apiPath, method, ca: k8sCa,
      headers: { Authorization: `Bearer ${k8sToken}` },
    }, res => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => { try { resolve(JSON.parse(body)); } catch (_) { resolve({}); } });
    });
    req.on('error', reject);
    req.end();
  });
}

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Health ----------
app.get('/api/health', (_req, res) => res.json({ status: 'ok' }));

// ---------- Config ----------
app.get('/api/config', (_req, res) => {
  res.json({
    argocdUrl:      process.env.ARGOCD_URL       || '',
    apicurioUiUrl:  process.env.APICURIO_UI_URL  || '',
    producerUrl:    process.env.PRODUCER_EXT_URL  || '',
    consumerUrl:    process.env.CONSUMER_EXT_URL  || '',
    apicurioApiUrl: process.env.APICURIO_API_URL  || '',
    consoleUrl:     process.env.OPENSHIFT_CONSOLE_URL || '',
    repoUrl:        process.env.REPO_URL         || '',
  });
});

// ---------- Producer proxy ----------
app.post('/api/producer/orders', async (req, res) => {
  try {
    const r = await fetch(`${PRODUCER_URL}/api/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    res.status(r.status).json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- Consumer proxy ----------
app.get('/api/consumer/orders', async (_req, res) => {
  try {
    const r = await fetch(`${CONSUMER_URL}/api/orders`);
    res.status(r.status).json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- Kafka Connect proxy ----------
app.get('/api/kafka-connect/connectors', async (_req, res) => {
  try {
    const r = await fetch(`${CONNECT_URL}/connectors?expand=status`);
    res.status(r.status).json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/kafka-connect/connectors/:name/status', async (req, res) => {
  try {
    const r = await fetch(`${CONNECT_URL}/connectors/${encodeURIComponent(req.params.name)}/status`);
    res.status(r.status).json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- Apicurio proxy ----------
app.all('/api/apicurio/*', async (req, res) => {
  try {
    const apiPath = req.params[0];
    const opts = { method: req.method, headers: {} };
    if (!['GET','HEAD'].includes(req.method) && req.body) {
      opts.headers['Content-Type'] = req.headers['content-type'] || 'application/json';
      opts.body = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
    }
    const r = await fetch(`${APICURIO_URL}/apis/registry/v3/${apiPath}`, opts);
    if (r.status === 204) return res.status(204).end();
    const ct = r.headers.get('content-type') || '';
    if (ct.includes('json')) return res.status(r.status).json(await r.json());
    res.status(r.status).send(await r.text());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- PostgreSQL ----------
app.get('/api/pg/customers', async (_req, res) => {
  try {
    const { rows } = await pgPool.query('SELECT * FROM customers ORDER BY id');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/pg/oracle-customers', async (_req, res) => {
  try {
    const { rows } = await pgPool.query('SELECT * FROM oracle_customers ORDER BY id');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pg/execute', async (req, res) => {
  try {
    const result = await pgPool.query(req.body.sql);
    res.json({ rowCount: result.rowCount, rows: result.rows || [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- Oracle ----------
async function oraExec(sql) {
  if (!oracledb) throw new Error('oracledb module not available');
  const conn = await oracledb.getConnection(ORA_CFG);
  try {
    const r = await conn.execute(sql, [], { autoCommit: true, outFormat: oracledb.OUT_FORMAT_OBJECT });
    return { rowsAffected: r.rowsAffected, rows: r.rows || [] };
  } finally { await conn.close(); }
}

app.get('/api/oracle/customers', async (_req, res) => {
  try {
    const r = await oraExec('SELECT ID, NAME, EMAIL, CITY FROM CUSTOMERS ORDER BY ID');
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/oracle/execute', async (req, res) => {
  try {
    res.json(await oraExec(req.body.sql));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---------- Kafka ----------
app.get('/api/kafka/topics', async (_req, res) => {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const topics = (await admin.listTopics()).filter(t => !t.startsWith('__')).sort();
    res.json(topics);
  } catch (e) { res.status(500).json({ error: e.message }); }
  finally { await admin.disconnect().catch(() => {}); }
});

app.get('/api/kafka/topics/:name', async (req, res) => {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const meta = await admin.fetchTopicMetadata({ topics: [req.params.name] });
    res.json(meta.topics[0] || {});
  } catch (e) { res.status(500).json({ error: e.message }); }
  finally { await admin.disconnect().catch(() => {}); }
});

app.get('/api/kafka/consume/:topic', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '10'), 50);
  const gid = `demo-ui-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const consumer = kafka.consumer({ groupId: gid });
  const msgs = [];
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: req.params.topic, fromBeginning: true });
    let idle;
    await new Promise(resolve => {
      const t = setTimeout(resolve, 6000);
      consumer.run({
        eachMessage: async ({ partition, message }) => {
          msgs.push({
            partition, offset: message.offset,
            key: message.key?.toString(),
            value: message.value?.toString(),
            timestamp: message.timestamp,
          });
          clearTimeout(idle);
          idle = setTimeout(() => { clearTimeout(t); resolve(); }, 500);
        },
      });
    });
    msgs.sort((a, b) => parseInt(b.timestamp) - parseInt(a.timestamp));
    res.json(msgs.slice(0, limit));
  } catch (e) { res.status(500).json({ error: e.message }); }
  finally { await consumer.disconnect().catch(() => {}); }
});

// ---------- Reset POC ----------
app.post('/api/reset', async (_req, res) => {
  const log = [];
  try {
    await pgPool.query('DELETE FROM customers');
    await pgPool.query('ALTER SEQUENCE customers_id_seq RESTART WITH 1');
    await pgPool.query(`INSERT INTO customers (name, email, city) VALUES
      ('Alice Silva',     'alice@techmart.com',   'São Paulo'),
      ('Bob Santos',      'bob@techmart.com',     'Rio de Janeiro'),
      ('Carlos Oliveira', 'carlos@techmart.com',  'Belo Horizonte')`);
    log.push('PostgreSQL customers: reset (3 registros iniciais)');
  } catch (e) { log.push(`PostgreSQL customers: erro — ${e.message}`); }

  try {
    await pgPool.query('DROP TABLE IF EXISTS oracle_customers');
    log.push('PostgreSQL oracle_customers: tabela removida');
  } catch (e) { log.push(`PostgreSQL oracle_customers: erro — ${e.message}`); }

  try {
    await oraExec('DELETE FROM CUSTOMERS');
    await oraExec("INSERT INTO CUSTOMERS (NAME, EMAIL, CITY) VALUES ('Maria Costa',    'maria@techmart.com', 'Curitiba')");
    await oraExec("INSERT INTO CUSTOMERS (NAME, EMAIL, CITY) VALUES ('João Pereira',   'joao@techmart.com',  'Porto Alegre')");
    await oraExec("INSERT INTO CUSTOMERS (NAME, EMAIL, CITY) VALUES ('Ana Lima',       'ana@techmart.com',   'Brasília')");
    log.push('Oracle customers: reset (3 registros iniciais)');
  } catch (e) { log.push(`Oracle customers: erro — ${e.message}`); }

  try {
    await fetch(`${APICURIO_URL}/apis/registry/v3/groups/default/artifacts/demo-order`, { method: 'DELETE' }).catch(() => {});
    log.push('Apicurio: artifact demo-order removido');
  } catch (_) {}

  try {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();
    if (topics.includes('orders')) { await admin.deleteTopics({ topics: ['orders'] }); }
    await admin.disconnect();
    log.push('Kafka: tópico orders removido');
  } catch (e) { log.push(`Kafka orders: erro — ${e.message}`); }

  if (k8sToken) {
    try {
      const pods = await k8sApi('GET', `/api/v1/namespaces/${k8sNs}/pods?labelSelector=app=kafka-consumer`);
      for (const pod of (pods.items || [])) {
        await k8sApi('DELETE', `/api/v1/namespaces/${k8sNs}/pods/${pod.metadata.name}`);
      }
      log.push('Consumer: pod reiniciado (pedidos limpos)');
    } catch (e) { log.push(`Consumer restart: erro — ${e.message}`); }
  }

  res.json({ status: 'ok', actions: log });
});

// SPA fallback
app.get('*', (_req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.listen(PORT, () => console.log(`Demo UI listening on :${PORT}`));
