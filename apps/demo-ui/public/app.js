/* Kafka Streaming Demo UI */

let CFG = {};
let step = 0;
let _polls = [];

const STEPS = [
  { t: 'Visão Geral',        s: 'Arquitetura e componentes' },
  { t: 'Kafka Básico',       s: 'Produzir e consumir eventos' },
  { t: 'Tópicos e Partições',s: 'Distribuição de mensagens' },
  { t: 'CDC PostgreSQL',     s: 'Change Data Capture' },
  { t: 'CDC Oracle',         s: 'LogMiner + Debezium' },
  { t: 'Sink Connector',     s: 'Oracle → PostgreSQL' },
  { t: 'Kafka Connect',      s: 'Visão operacional' },
  { t: 'Schema Registry',    s: 'Apicurio Registry' },
  { t: 'Data Contracts',     s: 'Compatibilidade e políticas' },
  { t: 'Resumo',             s: 'O que foi demonstrado' },
];

const RENDERS = [renderOverview, renderKafka, renderTopics, renderPgCDC, renderOraCDC, renderSink, renderConnect, renderSchema, renderContracts, renderSummary];

// ── Polling ──────────────────────────────────────────

function stopPolls() { _polls.forEach(clearInterval); _polls = []; }
function poll(fn, ms) { _polls.push(setInterval(fn, ms)); }

const STEP_INIT = [
  null,
  () => { loadOrders(); poll(() => loadOrders(true), 3000); },
  () => { listTopics(); describeTopic(); },
  () => { cdcTable('pg'); cdcEvents('pg','debezium.public.customers',true); poll(() => cdcTable('pg', true), 4000); poll(() => cdcEvents('pg','debezium.public.customers',true), 8000); },
  () => { cdcTable('ora'); cdcEvents('ora','oracle.DEBEZIUM.CUSTOMERS',true); poll(() => cdcTable('ora', true), 4000); poll(() => cdcEvents('ora','oracle.DEBEZIUM.CUSTOMERS',true), 8000); },
  () => { sinkSource(); sinkDest(); sinkStatus(true); poll(() => { sinkSource(true); sinkDest(true); sinkStatus(true); }, 5000); },
  () => { listConnectors(); poll(() => listConnectors(true), 5000); },
  () => { listArtifacts(); },
  null,
  null,
];

// ── Time tracking ────────────────────────────────────

const _seen = {};

function trackSeen(ns, key) {
  const k = `${ns}:${key}`;
  if (!_seen[k]) _seen[k] = Date.now();
  return _seen[k];
}

function timeAgo(ts) {
  const s = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (s < 5) return 'agora';
  if (s < 60) return `há ${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `há ${m}min`;
  const h = Math.floor(m / 60);
  if (h < 24) return `há ${h}h`;
  return `há ${Math.floor(h / 24)}d`;
}

function agoBadge(ts) {
  const s = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  const text = timeAgo(ts);
  if (s < 5) return `<span class="ago-badge ago-now">${text}</span>`;
  if (s < 30) return `<span class="ago-badge ago-recent">${text}</span>`;
  return `<span class="ago-badge ago-old">${text}</span>`;
}

// ── Helpers ──────────────────────────────────────────

const $ = id => document.getElementById(id);

async function api(path, opts = {}) {
  const r = await fetch(path, { headers: { 'Content-Type': 'application/json', ...opts.headers }, ...opts });
  if (r.status === 204) return {};
  const data = await r.json();
  if (!r.ok) throw new Error(data.error || data.message || data.detail || `HTTP ${r.status}`);
  return data;
}

function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function setHtml(id, html) { const el = $(id); if (el) { el.innerHTML = html; el.classList.remove('hidden'); } }

function showOk(id, data) {
  const txt = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
  setHtml(id, `<pre class="code">${esc(txt)}</pre>`);
}

function showErr(id, msg) {
  setHtml(id, `<div class="bg-red-50 border border-red-300 text-red-700 rounded-lg p-4 text-sm"><strong>Erro:</strong> ${esc(msg)}</div>`);
}

function showSpin(id) {
  setHtml(id, `<div class="flex items-center gap-2 text-gray-500 text-sm py-3"><span class="spinner"></span> Carregando...</div>`);
}

function showTable(id, headers, rows, rawCols) {
  if (!rows || !rows.length) return setHtml(id, '<p class="text-gray-500 italic text-sm py-2">Nenhum dado encontrado.</p>');
  const raw = rawCols ? new Set(rawCols) : null;
  const ths = headers.map(h => `<th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">${h}</th>`).join('');
  const trs = rows.map(r => `<tr class="hover:bg-gray-50">${r.map((c, ci) =>
    `<td class="px-3 py-2 text-sm whitespace-nowrap">${raw && raw.has(ci) ? (c || '') : (c != null ? esc(String(c)) : '<span class="text-gray-300">null</span>')}</td>`
  ).join('')}</tr>`).join('');
  setHtml(id, `<div class="overflow-x-auto rounded-lg border"><table class="min-w-full divide-y divide-gray-200"><thead class="bg-gray-50"><tr>${ths}</tr></thead><tbody class="divide-y divide-gray-200">${trs}</tbody></table></div>`);
}

function card(title, body, extra = '') {
  return `<div class="bg-white rounded-xl shadow-md p-6 mb-5 ${extra}">${title ? `<h3 class="text-base font-semibold text-gray-800 mb-4">${title}</h3>` : ''}${body}</div>`;
}

function liveCard(title, body) {
  return `<div class="bg-white rounded-xl shadow-md p-6 mb-5">
    <div class="flex items-center justify-between mb-4">
      <h3 class="text-base font-semibold text-gray-800">${title}</h3>
      <span class="live-badge"><span class="live-dot"></span>Ao vivo</span>
    </div>${body}</div>`;
}

function fNode(label, emoji, cls) {
  return `<div class="flex flex-col items-center px-4 py-3 rounded-lg border-2 min-w-[90px] ${cls}"><span class="text-xl mb-1">${emoji}</span><span class="text-xs font-semibold">${label}</span></div>`;
}

function flowDiagram(nodes) {
  return `<div class="flex items-center justify-center flex-wrap gap-2 py-5">${nodes.map((n, i) =>
    fNode(n[0], n[1], n[2]) + (i < nodes.length - 1 ? '<div class="text-gray-300 text-xl">→</div>' : '')
  ).join('')}</div>`;
}

function tip(text) {
  return `<div class="bg-amber-50 border-l-4 border-amber-400 p-4 rounded-r-lg mt-4"><div class="flex gap-3"><span class="text-lg">💡</span><div><p class="text-xs font-bold text-amber-800 mb-1 uppercase tracking-wide">Contexto técnico</p><p class="text-sm text-amber-700 leading-relaxed">${text}</p></div></div></div>`;
}

function pBtn(text, fn) { return `<button onclick="${fn}" class="bg-rh-red hover:bg-red-700 text-white text-sm font-medium rounded-lg px-4 py-2 transition-colors">${text}</button>`; }
function sBtn(text, fn) { return `<button onclick="${fn}" class="bg-gray-100 hover:bg-gray-200 text-gray-700 text-sm font-medium rounded-lg px-4 py-2 border border-gray-200 transition-colors">${text}</button>`; }
function dBtn(text, fn) { return `<button onclick="${fn}" class="bg-red-50 hover:bg-red-100 text-red-700 text-sm font-medium rounded-lg px-4 py-2 border border-red-300 transition-colors">${text}</button>`; }
function eLink(url, text) { return `<a href="${url}" target="_blank" class="inline-flex items-center gap-1.5 bg-teal-600 hover:bg-teal-700 text-white text-sm font-medium rounded-lg px-4 py-2 transition-colors">${text} ↗</a>`; }
function inp(id, label, ph, val = '') { return `<div><label class="block text-xs font-medium text-gray-600 mb-1">${label}</label><input id="${id}" placeholder="${ph}" value="${val}" class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-2 focus:ring-red-500 focus:border-red-500 outline-none"></div>`; }
function res(id) { return `<div id="${id}" class="hidden mt-3"></div>`; }

function scenario(text) {
  return `<div class="bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-xl p-5 mb-5">
    <div class="flex gap-3 items-start">
      <span class="text-2xl pt-0.5">🏪</span>
      <div class="text-sm text-gray-700 leading-relaxed">${text}</div>
    </div>
  </div>`;
}

// ── Under the Hood ───────────────────────────────────

const OC_KINDS = {
  Kafka:            'kafka.strimzi.io~v1beta2~Kafka',
  KafkaNodePool:    'kafka.strimzi.io~v1beta2~KafkaNodePool',
  KafkaTopic:       'kafka.strimzi.io~v1beta2~KafkaTopic',
  KafkaConnect:     'kafka.strimzi.io~v1beta2~KafkaConnect',
  KafkaConnector:   'kafka.strimzi.io~v1beta2~KafkaConnector',
  ApicurioRegistry: 'registry.apicur.io~v1~ApicurioRegistry3',
  Deployment:       'apps~v1~Deployment',
  Service:          'core~v1~Service',
  Secret:           'core~v1~Secret',
  ConfigMap:        'core~v1~ConfigMap',
  Route:            'route.openshift.io~v1~Route',
  NetworkPolicy:    'networking.k8s.io~v1~NetworkPolicy',
  PVC:              'core~v1~PersistentVolumeClaim',
  ServiceAccount:   'core~v1~ServiceAccount',
};

function ocLink(kind, name) {
  if (!CFG.consoleUrl) return null;
  const k = OC_KINDS[kind];
  if (!k) return `${CFG.consoleUrl}/k8s/ns/kafka-demo/${kind.toLowerCase()}s/${name}`;
  return `${CFG.consoleUrl}/k8s/ns/kafka-demo/${k}/${name}`;
}

function ghLink(templatePath) {
  const repo = CFG.repoUrl || 'https://github.com/dsferreira54/kafka-demo';
  return `${repo}/blob/main/gitops/templates/${templatePath}`;
}

const HOOD_ICONS = {
  Kafka:            ['K', 'bg-red-100 text-red-700'],
  KafkaNodePool:    ['NP', 'bg-red-50 text-red-600'],
  KafkaTopic:       ['T',  'bg-red-50 text-red-600'],
  KafkaConnect:     ['KC', 'bg-purple-100 text-purple-700'],
  KafkaConnector:   ['Co', 'bg-teal-100 text-teal-700'],
  ApicurioRegistry: ['AR', 'bg-purple-100 text-purple-700'],
  Deployment:       ['D',  'bg-blue-100 text-blue-700'],
  Service:          ['S',  'bg-gray-100 text-gray-700'],
  Secret:           ['Se', 'bg-yellow-100 text-yellow-700'],
  ConfigMap:        ['CM', 'bg-gray-100 text-gray-600'],
  Route:            ['R',  'bg-green-100 text-green-700'],
  NetworkPolicy:    ['NP', 'bg-gray-100 text-gray-600'],
};

function hoodResource(kind, name, desc, templatePath) {
  const [letter, cls] = HOOD_ICONS[kind] || ['?', 'bg-gray-100 text-gray-600'];
  const oc = ocLink(kind, name);
  const gh = templatePath ? ghLink(templatePath) : null;
  return `<div class="hood-res">
    <div class="hood-icon ${cls}">${letter}</div>
    <div class="min-w-0">
      <div class="hood-name">${esc(name)}</div>
      <div class="hood-desc">${desc}</div>
    </div>
    <div class="hood-links">
      ${oc ? `<a href="${oc}" target="_blank" class="hood-link hood-link-oc">OpenShift</a>` : ''}
      ${gh ? `<a href="${gh}" target="_blank" class="hood-link hood-link-gh">YAML</a>` : ''}
    </div>
  </div>`;
}

function hoodFlow(nodes) {
  return `<div class="hood-flow">${nodes.map((n, i) =>
    `<span class="hood-flow-node ${n[1]}">${n[0]}</span>${i < nodes.length - 1 ? '<span class="hood-flow-arrow">→</span>' : ''}`
  ).join('')}</div>`;
}

function underTheHood(flowHtml, resources, explanation) {
  return `<div class="hood-card mt-5">
    <div class="hood-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/></svg>Debaixo do Capô</div>
    ${flowHtml}
    ${explanation ? `<p class="text-xs text-gray-500 leading-relaxed mb-3">${explanation}</p>` : ''}
    <div class="divide-y divide-gray-100">${resources}</div>
  </div>`;
}

// ── Confluent Comparison ─────────────────────────────

const CF_BADGES = {
  direct:  ['Direta',                'cf-badge-direct'],
  close:   ['Muito próxima',         'cf-badge-close'],
  good:    ['Boa correspondência',   'cf-badge-good'],
  strong:  ['Correspondência forte', 'cf-badge-good'],
  partial: ['Parcial',               'cf-badge-partial'],
  gap:     ['Sem equivalente direto','cf-badge-gap'],
};

function cfRow(confluent, redhat, badgeKey, note) {
  const [label, cls] = CF_BADGES[badgeKey] || CF_BADGES.partial;
  return `<div class="cf-item">
    <div class="cf-item-head"><span class="cf-badge ${cls}">${label}</span></div>
    <div class="cf-versus">
      <div class="cf-side cf-side-left">
        <div class="cf-side-icon">☁️ Confluent Cloud</div>
        <div class="cf-side-text">${confluent}</div>
      </div>
      <div class="cf-arrow">⇄</div>
      <div class="cf-side cf-side-right">
        <div class="cf-side-icon">🎩 Red Hat (esta demo)</div>
        <div class="cf-side-text">${redhat}</div>
      </div>
    </div>${note ? `\n    <div class="cf-note"><span>${note}</span></div>` : ''}
  </div>`;
}

function confluentComparison(rows, summary) {
  return `<div class="cf-card mt-6">
    <div class="cf-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-3"/><path d="M18 3h3v3"/><path d="M10 14L21 3"/></svg>E no Confluent?</div>
    ${rows}
    ${summary ? `<div class="cf-summary">${summary}</div>` : ''}
  </div>`;
}

function compCard(name, desc, emoji, c) {
  return `<div class="flex items-center gap-3 p-3 rounded-lg bg-${c}-50 border border-${c}-200"><span class="text-xl">${emoji}</span><div><p class="font-medium text-sm">${name}</p><p class="text-xs text-gray-500">${desc}</p></div></div>`;
}

// ── Step 0: Overview ─────────────────────────────────

function archDiagram() {
  const n = (label, sub, cls, icon) =>
    `<div class="arch-node ${cls}"><span class="arch-icon">${icon}</span><span class="arch-label">${label}</span><span class="arch-sub">${sub}</span></div>`;
  const arrow = (dir = 'right') =>
    `<div class="arch-arrow arch-arrow-${dir}"><svg viewBox="0 0 24 12" fill="none" stroke="currentColor" stroke-width="2"><path d="M0 6h20M16 1l5 5-5 5"/></svg></div>`;
  const arrowDown = () =>
    `<div class="arch-arrow-down"><svg viewBox="0 0 12 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 0v20M1 16l5 5 5-5"/></svg></div>`;

  return `<div class="arch-container">
    <div class="arch-top-row">
      ${n('Producer','Quarkus + SmallRye','arch-blue','📤')}
      ${arrow()}
      ${n('Apache Kafka','3 brokers · KRaft','arch-red','📨')}
      ${arrow()}
      ${n('Consumer','Quarkus + SmallRye','arch-blue','📥')}
    </div>

    <div class="arch-schema-float">
      ${n('Apicurio Registry','Schemas · Data Contracts','arch-purple','📋')}
      <div class="arch-schema-line"></div>
    </div>

    <div class="arch-mid-label"><span>Kafka Connect</span></div>

    <div class="arch-connect-row">
      <div class="arch-connect-group">
        ${arrowDown()}
        ${n('Debezium CDC','Source Connector','arch-green','🔄')}
        ${arrowDown()}
        ${n('PostgreSQL 12','Banco operacional','arch-indigo','🐘')}
      </div>
      <div class="arch-connect-group">
        ${arrowDown()}
        ${n('JDBC Sink','Sink Connector','arch-teal','⬇️')}
        ${arrowDown()}
        ${n('PostgreSQL','Destino (réplica)','arch-indigo','🐘')}
      </div>
      <div class="arch-connect-group">
        ${arrowDown()}
        ${n('Debezium CDC','Source Connector','arch-green','🔄')}
        ${arrowDown()}
        ${n('Oracle 23c Free','Banco legado','arch-amber','🔶')}
      </div>
    </div>
  </div>`;
}

function renderOverview() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Kafka Streaming Demo</h2><p class="text-gray-500 mt-1">Demonstração de uma arquitetura de streaming baseada no stack Red Hat.</p></div>

    ${card('Cenário', `
      <div class="flex gap-4 items-start">
        <div class="text-3xl pt-1">🏪</div>
        <div class="text-sm text-gray-700 leading-relaxed space-y-2">
          <p>A <strong>TechMart</strong> é uma rede varejista de eletrônicos que opera com dois sistemas de banco de dados: um <strong>PostgreSQL</strong> para o e-commerce (cadastro de clientes, carrinho, etc.) e um <strong>Oracle</strong> legado que gerencia o ERP (Enterprise Resource Planning — sistema de gestão integrada) de lojas físicas.</p>
          <p>Hoje, a sincronização entre os dois bancos é feita por batch noturno, o que gera atrasos e inconsistências. A proposta é adotar uma <strong>arquitetura de streaming com Apache Kafka</strong> para capturar mudanças em tempo real (CDC), integrar os sistemas e garantir governança dos dados com schemas e contratos.</p>
          <p>Nesta demo, demonstramos como o <strong>stack Red Hat</strong> — Streams for Apache Kafka, Debezium, Apicurio Registry — resolve esse problema, desde a publicação de pedidos até a replicação cross-database e a evolução segura de schemas.</p>
        </div>
      </div>
    `)}

    ${card('Arquitetura', archDiagram())}

    ${card('Componentes', `<div class="grid grid-cols-2 lg:grid-cols-3 gap-3">
      ${compCard('Apache Kafka','3 brokers, KRaft mode','📨','red')}
      ${compCard('Producer','Quarkus + SmallRye','📤','blue')}
      ${compCard('Consumer','Quarkus + SmallRye','📥','blue')}
      ${compCard('PostgreSQL','v12, CDC source','🐘','indigo')}
      ${compCard('Oracle','23c Free, LogMiner','🔶','amber')}
      ${compCard('Debezium','v3.6.1 (PG + Oracle)','🔄','green')}
      ${compCard('JDBC Sink','Oracle → PG','⬇️','teal')}
      ${compCard('Apicurio','Registry v3, KafkaSQL','📋','purple')}
      ${compCard('ArgoCD','GitOps deploy','🚀','gray')}
    </div>`)}

    ${card('Links Externos', `<div class="flex flex-wrap gap-3">
      ${CFG.argocdUrl ? eLink(CFG.argocdUrl, 'ArgoCD Console') : ''}
      ${CFG.apicurioUiUrl ? eLink(CFG.apicurioUiUrl, 'Apicurio Registry UI') : ''}
      ${CFG.producerUrl ? eLink(CFG.producerUrl + '/q/health', 'Producer Health') : ''}
      ${CFG.consumerUrl ? eLink(CFG.consumerUrl + '/q/health', 'Consumer Health') : ''}
    </div>`)}

    ${tip('Esta demo demonstra como o stack Red Hat pode substituir uma plataforma Confluent. O percurso cobre: publicação e consumo de eventos (pub/sub), captura de mudanças em PostgreSQL e Oracle via Debezium (CDC), replicação cross-database com Sink Connector, e governança de dados com Schema Registry e Data Contracts no Apicurio.')}
  </div>`;
}

// ── Step 1: Kafka Basics ─────────────────────────────

function renderKafka() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Kafka: Produzir e Consumir</h2><p class="text-gray-500 mt-1">Demonstração do fluxo básico de mensageria: publicar um pedido via Producer e consumi-lo via Consumer.</p></div>

    ${scenario('Quando um cliente da <strong>TechMart</strong> finaliza uma compra no e-commerce, o sistema de checkout precisa notificar em tempo real os serviços de <strong>fulfillment</strong> (separação e envio), <strong>estoque</strong> (reserva de produtos) e <strong>notificação</strong> (e-mail de confirmação). Aqui, o Producer representa o checkout publicando o pedido no Kafka, e o Consumer representa um desses serviços downstream processando o evento.')}

    ${flowDiagram([['Producer','📤','bg-blue-50 border-blue-200'],['Kafka','📨','bg-red-50 border-red-200'],['Consumer','📥','bg-green-50 border-green-200']])}

    ${card('Enviar Pedido', `
      <div class="grid grid-cols-2 gap-3 mb-4">
        ${inp('o-name','Cliente','Maria Silva','Maria Silva')}
        ${inp('o-prod','Produto','Red Hat OpenShift','Red Hat OpenShift')}
        ${inp('o-qty','Quantidade','1','1')}
        ${inp('o-price','Preço','15000','15000')}
      </div>
      <div class="flex gap-2">${pBtn('🚀 Enviar Pedido','sendOrder()')}</div>
      ${res('r-send')}
    `)}

    ${liveCard('Pedidos Consumidos', `
      <span id="r-count" class="text-xs text-gray-400 mb-2 block"></span>
      <div id="r-orders"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando pedidos...</div></div>
    `)}

    ${tip('"O Producer é uma aplicação Quarkus que recebe pedidos via REST e publica no tópico <code>orders</code> do Kafka usando SmallRye Reactive Messaging. O Consumer consome do mesmo tópico e expõe via REST. Isso demonstra o padrão pub/sub: publisher → broker → subscriber."')}

    ${underTheHood(
      hoodFlow([
        ['Producer (Quarkus)', 'bg-blue-50 border-blue-200 text-blue-800'],
        ['Kafka Cluster', 'bg-red-50 border-red-200 text-red-800'],
        ['Tópico orders', 'bg-red-50 border-red-200 text-red-700'],
        ['Consumer (Quarkus)', 'bg-blue-50 border-blue-200 text-blue-800'],
      ]),
      hoodResource('Kafka', 'kafka-cluster', 'Cluster Kafka com 3 brokers em modo KRaft — define listeners, storage e replicação', '1-amq-streams/2-kafka.yaml') +
      hoodResource('KafkaTopic', 'orders', 'Tópico <code>orders</code> com 3 partições e fator de replicação 3', '1-amq-streams/3-topics.yaml') +
      hoodResource('Deployment', 'kafka-producer', 'Aplicação Quarkus que expõe REST <code>/api/orders</code> e publica no Kafka via SmallRye', '3-apps/0-producer.yaml') +
      hoodResource('Deployment', 'kafka-consumer', 'Aplicação Quarkus que consome do tópico <code>orders</code> e expõe via REST', '3-apps/1-consumer.yaml') +
      hoodResource('Route', 'kafka-producer', 'Rota externa para o Producer', '3-apps/0-producer.yaml') +
      hoodResource('Route', 'kafka-consumer', 'Rota externa para o Consumer', '3-apps/1-consumer.yaml'),
      'O <strong>Kafka CR</strong> instrui o Strimzi Operator a criar os 3 brokers como StatefulSet. O <strong>KafkaTopic CR</strong> cria o tópico <code>orders</code>. O Producer recebe pedidos via REST e usa SmallRye Reactive Messaging para publicá-los. O Consumer lê do mesmo tópico e armazena em memória.'
    )}

    ${confluentComparison(
      cfRow(
        'Confluent Cloud Dedicated (1 CKU) — SaaS totalmente gerenciado na GCP',
        'Red Hat Streams for Apache Kafka — Kafka auto-gerenciado via Strimzi Operator no OpenShift',
        'direct',
        'O motor é o mesmo Apache Kafka. A diferença é operacional: no Confluent, a infraestrutura é invisível (SaaS); no Red Hat, você declara o cluster via CRDs e o Strimzi Operator cuida do lifecycle — mas você tem controle total sobre configuração, versão e topologia.'
      ) +
      cfRow(
        'API Keys + Service Accounts para autenticação e ACLs',
        'KafkaUser CRD + TLS/SCRAM-SHA + ACLs declarativas',
        'good',
        'Ambos oferecem autenticação e autorização granular. No Confluent, credenciais são gerenciadas via UI/CLI da plataforma. No Red Hat, <code>KafkaUser</code> CRDs permitem definir usuários e ACLs como código, integrados ao GitOps.'
      ),
      'Em um ambiente Confluent Cloud típico, o cluster Dedicated hospeda centenas de tópicos com vazão contínua. O Kafka em si é a peça com migração mais direta — a mudança principal é assumir a operação do cluster.'
    )}
  </div>`;
}

// ── Step 2: Topics ───────────────────────────────────

function renderTopics() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Tópicos e Partições</h2><p class="text-gray-500 mt-1">Explorar os tópicos criados no cluster Kafka, incluindo tópicos de aplicação e tópicos de CDC.</p></div>

    ${scenario('A <strong>TechMart</strong> processa milhares de pedidos por dia em horários de pico (Black Friday, por exemplo). As <strong>partições</strong> permitem que múltiplos consumidores processem pedidos em paralelo — cada instância do serviço de fulfillment lê de uma partição diferente. A <strong>chave</strong> do pedido (ID do cliente) garante que todos os pedidos de um mesmo cliente vão para a mesma partição, preservando a ordem de processamento.')}

    ${card('Tópicos do Cluster', `
      <div id="r-topics"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando tópicos...</div></div>
    `)}

    ${card('Detalhes do Tópico', `
      <div class="flex gap-2 items-end mb-2">
        ${inp('t-name','Nome do Tópico','orders','orders')}
        <div class="pb-0.5">${sBtn('🔍 Detalhar','describeTopic()')}</div>
      </div>
      <div id="r-topic-detail"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando...</div></div>
    `)}

    ${tip('"Cada tópico é dividido em partições. As partições permitem paralelismo no consumo e distribuição de carga entre brokers. Mensagens com a mesma chave vão para a mesma partição, garantindo ordenação por chave. O fator de replicação define quantas cópias existem para alta disponibilidade."')}

    ${underTheHood(
      hoodFlow([
        ['Kafka CR', 'bg-red-50 border-red-200 text-red-800'],
        ['KafkaNodePool', 'bg-red-50 border-red-200 text-red-700'],
        ['3 Brokers (StatefulSet)', 'bg-gray-100 border-gray-300 text-gray-700'],
        ['KafkaTopic CRs', 'bg-red-50 border-red-200 text-red-700'],
      ]),
      hoodResource('Kafka', 'kafka-cluster', 'Define o cluster: versão Kafka 4.2.0, modo KRaft (sem ZooKeeper), listeners e autorização', '1-amq-streams/2-kafka.yaml') +
      hoodResource('KafkaNodePool', 'combined', 'Pool de 3 nós com roles broker+controller, storage de 10Gi cada', '1-amq-streams/1-kafka-node-pool.yaml') +
      hoodResource('KafkaTopic', 'orders', 'Tópico <code>orders</code> — partições e replicação definidos declarativamente', '1-amq-streams/3-topics.yaml'),
      'O <strong>Kafka CR</strong> define a configuração do cluster (KRaft, listeners). O <strong>KafkaNodePool CR</strong> especifica quantos nós, seus roles e storage. O Strimzi Operator reconcilia esses CRs e cria um StatefulSet com 3 pods. Os <strong>KafkaTopic CRs</strong> criam os tópicos declarativamente — o Entity Topic Operator monitora esses CRs e gerencia os tópicos no cluster.'
    )}

    ${confluentComparison(
      cfRow(
        'Gerenciamento de tópicos e partições via UI/CLI do Confluent Cloud',
        'KafkaTopic CRDs declarativos — tópicos como código, versionados no Git',
        'direct',
        'A semântica de tópicos e partições é idêntica (mesmo Apache Kafka). No Confluent, tópicos são gerenciados via console web; no Red Hat, são declarados como <code>KafkaTopic</code> CRDs e reconciliados pelo operator — o que permite GitOps nativo.'
      ) +
      cfRow(
        'A maioria dos tópicos CDC é mono-particionada (padrão do Debezium)',
        'Mesma distribuição — particionamento é configuração do tópico, independente da plataforma',
        'direct',
        'O padrão de partição única nos tópicos CDC garante ordenação por chave primária. Na migração, essa configuração seria preservada. Para tópicos com necessidade de maior paralelismo, ambas as plataformas permitem aumentar partições.'
      ),
      'Em ambientes com forte uso de CDC, a maioria dos tópicos tende a ter apenas 1 partição (exigência de ordenação por chave primária). Isso concentra a carga em poucos tópicos — um ponto que precisa entrar no desenho de sizing de qualquer migração.'
    )}
  </div>`;
}

// ── Generic CDC renderer ─────────────────────────────

function cdcStep(opts) {
  const { title, desc, dbLabel, dbEmoji, dbColor, apiBase, cdcTopic, tipText, scenarioText, hoodHtml, cfHtml } = opts;
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">${title}</h2><p class="text-gray-500 mt-1">${desc}</p></div>

    ${scenarioText ? scenario(scenarioText) : ''}

    ${flowDiagram([[dbLabel, dbEmoji, `bg-${dbColor}-50 border-${dbColor}-200`],['Debezium','🔄','bg-green-50 border-green-200'],['Kafka','📨','bg-red-50 border-red-200']])}

    ${card('Inserir Registro', `
      <div class="grid grid-cols-3 gap-3 mb-4">
        ${inp(`${apiBase}-name`,'Nome','Ana Costa','Ana Costa')}
        ${inp(`${apiBase}-email`,'Email','ana@example.com','ana@example.com')}
        ${inp(`${apiBase}-city`,'Cidade','São Paulo','São Paulo')}
      </div>
      ${pBtn('➕ Inserir',"cdcInsert('" + apiBase + "')")}
      ${res(`r-${apiBase}-insert`)}
    `)}

    ${card('Atualizar / Remover', `
      <div class="grid grid-cols-3 gap-3 mb-4">
        ${inp(`${apiBase}-upd-id`,'ID do Registro','1','')}
        ${inp(`${apiBase}-upd-field`,'Campo','email','')}
        ${inp(`${apiBase}-upd-val`,'Novo Valor','novo@example.com','')}
      </div>
      <div class="flex gap-2">
        ${sBtn('✏️ Atualizar',"cdcUpdate('" + apiBase + "')")}
        ${dBtn('🗑️ Remover',"cdcDelete('" + apiBase + "')")}
      </div>
      ${res(`r-${apiBase}-mod`)}
    `)}

    ${liveCard('Dados Atuais', `
      <div id="r-${apiBase}-table"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando dados...</div></div>
    `)}

    ${liveCard('Eventos CDC no Kafka', `
      <p class="text-xs text-gray-500 mb-3">Tópico: <code class="bg-gray-100 px-1.5 py-0.5 rounded">${cdcTopic}</code></p>
      <div id="r-${apiBase}-cdc"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando eventos CDC...</div></div>
    `)}

    ${tip(tipText)}

    ${hoodHtml || ''}
    ${cfHtml || ''}
  </div>`;
}

function renderPgCDC() {
  return cdcStep({
    title: 'CDC PostgreSQL',
    desc: 'Captura de mudanças no PostgreSQL via Debezium. Cada INSERT, UPDATE ou DELETE gera um evento no Kafka.',
    dbLabel: 'PostgreSQL', dbEmoji: '🐘', dbColor: 'indigo',
    apiBase: 'pg', cdcTopic: 'debezium.public.customers',
    scenarioText: 'O <strong>PostgreSQL</strong> é o banco do e-commerce da <strong>TechMart</strong>, onde ficam os cadastros de clientes online. Quando a equipe de CRM atualiza um e-mail, o marketing cria uma segmentação ou o suporte corrige um endereço, essas mudanças precisam ser propagadas <strong>em tempo real</strong> para analytics, personalização e outros microsserviços — sem alterar o código da aplicação original. O Debezium resolve isso capturando cada operação diretamente do log de transações do banco.',
    tipText: '"O Debezium usa logical replication do PostgreSQL (pgoutput plugin) para capturar mudanças em tempo real. Cada operação no banco gera um evento com o tipo de operação (c=create, u=update, d=delete), os dados antes e depois, e metadados da origem."',
    hoodHtml: underTheHood(
      hoodFlow([
        ['PostgreSQL', 'bg-indigo-50 border-indigo-200 text-indigo-800'],
        ['WAL (pgoutput)', 'bg-indigo-50 border-indigo-200 text-indigo-600'],
        ['Debezium Connector', 'bg-green-50 border-green-200 text-green-800'],
        ['KafkaConnect', 'bg-purple-50 border-purple-200 text-purple-800'],
        ['Kafka Topic', 'bg-red-50 border-red-200 text-red-800'],
      ]),
      hoodResource('Deployment', 'postgresql', 'PostgreSQL 12 com <code>wal_level=logical</code> habilitado para CDC', '4-databases/0-postgresql.yaml') +
      hoodResource('Secret', 'postgresql-secret', 'Credenciais do banco (usuário, senha, nome do banco)', '4-databases/0-postgresql.yaml') +
      hoodResource('ConfigMap', 'postgresql-config', 'Configuração customizada: <code>wal_level=logical</code>', '4-databases/0-postgresql.yaml') +
      hoodResource('KafkaConnect', 'kafka-connect', 'Cluster Kafka Connect com plugins Debezium compilados via build S2I', '5-kafka-connect/0-kafka-connect.yaml') +
      hoodResource('KafkaConnector', 'debezium-postgres', 'Source Connector: lê WAL do PostgreSQL e publica em <code>debezium.public.customers</code>', '5-kafka-connect/1-pg-connector.yaml'),
      'O <strong>Deployment</strong> do PostgreSQL é configurado com <code>wal_level=logical</code> via ConfigMap. O <strong>KafkaConnect CR</strong> instrui o Strimzi a construir uma imagem Docker com os plugins Debezium. O <strong>KafkaConnector CR</strong> <code>debezium-postgres</code> configura a conexão ao banco, o slot de replicação (<code>debezium_slot</code>) e o prefixo de tópico (<code>debezium</code>). O Debezium lê o WAL e publica eventos no tópico <code>debezium.public.customers</code>.'
    ),
    cfHtml: confluentComparison(
      cfRow(
        'PostgreSQL CDC Source V2 (fully-managed) — usa Debezium internamente',
        'Red Hat build of Debezium — PostgreSQL Connector (mesmo engine)',
        'close',
        'A própria Confluent documenta que o conector CDC V2 usa Debezium internamente. Isso significa que o motor de captura é o mesmo — a migração envolve mapeamento de configuração (offsets, snapshot mode, slot name), não troca de tecnologia.'
      ),
      'O conector PostgreSQL CDC V2 da Confluent já usa Debezium internamente. Por isso, este é provavelmente o caminho de migração mais direto entre todos os conectores — a mudança é de configuração, não de engine.'
    ),
  });
}

function renderOraCDC() {
  return cdcStep({
    title: 'CDC Oracle',
    desc: 'Captura de mudanças no Oracle Database 23c via Debezium com LogMiner.',
    dbLabel: 'Oracle', dbEmoji: '🔶', dbColor: 'amber',
    apiBase: 'ora', cdcTopic: 'oracle.DEBEZIUM.CUSTOMERS',
    scenarioText: 'O <strong>Oracle</strong> é o banco legado do ERP de lojas físicas da <strong>TechMart</strong>. Quando um vendedor cadastra ou atualiza um cliente no sistema da loja, essa informação fica "presa" no Oracle. Para construir uma <strong>visão unificada do cliente</strong> (loja + e-commerce), a TechMart precisa capturar essas mudanças sem modificar o ERP legado. O Debezium com LogMiner faz isso lendo diretamente os redo logs do Oracle.',
    tipText: '"O Debezium usa Oracle LogMiner para capturar mudanças, sem necessidade de triggers ou polling. O LogMiner lê os redo logs do banco e extrai as operações DML. É a mesma tecnologia usada pelo CDC em produção, mas aqui com Oracle 23c Free."',
    hoodHtml: underTheHood(
      hoodFlow([
        ['Oracle 23c', 'bg-amber-50 border-amber-200 text-amber-800'],
        ['Redo Logs (LogMiner)', 'bg-amber-50 border-amber-200 text-amber-600'],
        ['Debezium Connector', 'bg-green-50 border-green-200 text-green-800'],
        ['KafkaConnect', 'bg-purple-50 border-purple-200 text-purple-800'],
        ['Kafka Topic', 'bg-red-50 border-red-200 text-red-800'],
      ]),
      hoodResource('Deployment', 'oracle', 'Oracle Database Free 23c com ARCHIVELOG e supplemental logging habilitados', '4-databases/1-oracle.yaml') +
      hoodResource('Secret', 'oracle-secret', 'Senha do banco Oracle', '4-databases/1-oracle.yaml') +
      hoodResource('ServiceAccount', 'oracle-sa', 'ServiceAccount com SCC <code>anyuid</code> (Oracle precisa rodar como root)', '4-databases/1-oracle.yaml') +
      hoodResource('KafkaConnect', 'kafka-connect', 'Cluster Kafka Connect com plugin <code>debezium-connector-oracle</code>', '5-kafka-connect/0-kafka-connect.yaml') +
      hoodResource('KafkaConnector', 'debezium-oracle', 'Source Connector: lê redo logs via LogMiner e publica em <code>oracle.DEBEZIUM.CUSTOMERS</code>', '5-kafka-connect/2-oracle-connector.yaml'),
      'O <strong>Deployment</strong> do Oracle usa a imagem <code>gvenzl/oracle-free</code> com um ServiceAccount que possui SCC <code>anyuid</code> (Oracle precisa de UID fixo). O banco é configurado com ARCHIVELOG e supplemental logging. O <strong>KafkaConnector CR</strong> <code>debezium-oracle</code> usa um usuário comum <code>C##DBZUSER</code> no CDB com permissões de LogMiner para capturar mudanças do PDB <code>FREEPDB1</code>.'
    ),
    cfHtml: confluentComparison(
      cfRow(
        'Confluent Oracle CDC Source (fully-managed)',
        'Red Hat build of Debezium — Oracle Connector via LogMiner',
        'good',
        'Ambos fazem CDC de Oracle. O Debezium suportado pela Red Hat usa LogMiner para ler redo logs. A migração exige mapeamento de configuração, atenção à retomada do CDC na virada (offsets, snapshot), e validação de permissões do usuário de captura.'
      ),
      'Em ambientes com Oracle como fonte principal de CDC, o conector Oracle tende a ser o maior gerador de dados do cluster. A migração é funcional, mas precisa de planejamento cuidadoso por conta do volume e da sensibilidade da captura em tempo real.'
    ),
  });
}

// ── Step 5: Sink Connector ───────────────────────────

function renderSink() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Sink Connector</h2><p class="text-gray-500 mt-1">Dados fluem do Oracle via CDC para o Kafka e são escritos automaticamente no PostgreSQL pelo JDBC Sink Connector.</p></div>

    ${scenario('Este é o cenário que a <strong>TechMart</strong> mais quer resolver: hoje, os clientes cadastrados nas <strong>lojas físicas</strong> (Oracle) só aparecem no <strong>e-commerce</strong> (PostgreSQL) após um batch noturno. Com o Sink Connector, essa replicação é <strong>automática e em tempo real</strong> — um cliente cadastrado na loja às 14h já pode fazer login no e-commerce às 14h01. Aqui demonstramos esse fluxo: Oracle → Debezium → Kafka → JDBC Sink → PostgreSQL.')}

    ${flowDiagram([['Oracle','🔶','bg-amber-50 border-amber-200'],['Debezium','🔄','bg-green-50 border-green-200'],['Kafka','📨','bg-red-50 border-red-200'],['JDBC Sink','⬇️','bg-teal-50 border-teal-200'],['PostgreSQL','🐘','bg-indigo-50 border-indigo-200']])}

    ${liveCard('Dados de Origem (Oracle → CUSTOMERS)', `
      <div id="r-sink-src"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando...</div></div>
    `)}

    ${liveCard('Dados de Destino (PostgreSQL → oracle_customers)', `
      <div id="r-sink-dst"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando...</div></div>
    `)}

    ${liveCard('Status do Connector', `
      <div id="r-sink-status"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando...</div></div>
    `)}

    ${tip('"O JDBC Sink Connector do Debezium consome eventos CDC do tópico Oracle e os replica automaticamente numa tabela PostgreSQL. Isso demonstra o conceito de Sink Connector, análogo ao fluxo Kafka → Google Cloud Storage do ambiente real. O modo upsert garante idempotência: inserções e atualizações são tratadas corretamente."')}

    ${underTheHood(
      hoodFlow([
        ['Oracle', 'bg-amber-50 border-amber-200 text-amber-800'],
        ['debezium-oracle', 'bg-green-50 border-green-200 text-green-800'],
        ['Kafka Topic', 'bg-red-50 border-red-200 text-red-800'],
        ['jdbc-sink', 'bg-teal-50 border-teal-200 text-teal-800'],
        ['PostgreSQL', 'bg-indigo-50 border-indigo-200 text-indigo-800'],
      ]),
      hoodResource('KafkaConnector', 'debezium-oracle', 'Source: captura mudanças do Oracle e publica em <code>oracle.DEBEZIUM.CUSTOMERS</code>', '5-kafka-connect/2-oracle-connector.yaml') +
      hoodResource('KafkaConnector', 'jdbc-sink-oracle-to-pg', 'Sink: consome do tópico Oracle e escreve na tabela <code>oracle_customers</code> do PostgreSQL', '5-kafka-connect/3-jdbc-sink.yaml') +
      hoodResource('KafkaConnect', 'kafka-connect', 'Cluster Connect que executa ambos os conectores como tasks', '5-kafka-connect/0-kafka-connect.yaml') +
      hoodResource('Deployment', 'postgresql', 'Banco de destino — tabela <code>oracle_customers</code> criada automaticamente pelo Sink', '4-databases/0-postgresql.yaml'),
      'O <strong>debezium-oracle</strong> (Source Connector) captura mudanças do Oracle e publica no tópico <code>oracle.DEBEZIUM.CUSTOMERS</code> com schema embutido (<code>schemas.enable: true</code>). O <strong>jdbc-sink-oracle-to-pg</strong> (Sink Connector) consome desse tópico e escreve no PostgreSQL usando modo <code>upsert</code> com chave primária derivada do <code>record_key</code>. O Sink cria a tabela automaticamente se não existir.'
    )}

    ${confluentComparison(
      cfRow(
        'GCS Sink Connector (fully-managed) — 11 conectores exportando para Google Cloud Storage',
        'JDBC Sink Connector — substituto educacional (Kafka → PostgreSQL)',
        'gap',
        'Nesta demo, usamos o JDBC Sink como analogia do fluxo Kafka → destino externo. Em cenários reais, conectores GCS Sink exportam dados para o Google Cloud Storage. Este é o <strong>principal gap funcional</strong> de uma migração: não existe um conector GCS equivalente productizado no stack Red Hat. A alternativa seria Red Hat build of Apache Camel ou um conector customizado — requer validação.'
      ) +
      cfRow(
        'PubSub Source Connector (fully-managed) — 1 conector ingerindo do Google Pub/Sub',
        'Não demonstrado nesta demo',
        'gap',
        'O Confluent oferece um conector Pub/Sub Source fully-managed. No ecossistema Red Hat, o Apache Camel possui componente Google Pubsub para essa integração, mas não é um drop-in — vira uma integração Camel, não uma migração direta.'
      ),
      'Os conectores GCS Sink e Pub/Sub Source são proprietários da Confluent e representam os pontos que exigem mais trabalho na migração. O Tanaka corretamente identificou que precisam de análise à parte, possivelmente via Camel.'
    )}
  </div>`;
}

// ── Step 6: Kafka Connect ────────────────────────────

function renderConnect() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Kafka Connect</h2><p class="text-gray-500 mt-1">Visão operacional do cluster Kafka Connect e dos conectores configurados.</p></div>

    ${scenario('A equipe de plataforma da <strong>TechMart</strong> é responsável por garantir que todas as integrações de dados estejam operacionais. Em um ambiente real, poderiam existir dezenas de conectores (CDC de vários bancos, sinks para data lakes, webhooks). Esta visão operacional mostra o <strong>status de cada conector e suas tasks</strong>, permitindo identificar rapidamente se alguma integração parou ou está degradada.')}

    ${flowDiagram([['Kafka Connect','🔌','bg-purple-50 border-purple-200'],['Connectors','🔗','bg-teal-50 border-teal-200'],['Tasks','⚙️','bg-gray-100 border-gray-300']])}

    ${liveCard('Conectores', `
      <div id="r-connectors"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando conectores...</div></div>
    `)}

    ${tip('"Kafka Connect é o framework de integração do Kafka. Ele gerencia Source Connectors (que trazem dados para o Kafka) e Sink Connectors (que levam dados do Kafka para fora). Cada connector pode ter múltiplas tasks executando em paralelo. O Strimzi operator gerencia o lifecycle dos connectors via KafkaConnector CRDs."')}

    ${underTheHood(
      hoodFlow([
        ['KafkaConnect CR', 'bg-purple-50 border-purple-200 text-purple-800'],
        ['Strimzi Build (S2I)', 'bg-gray-100 border-gray-300 text-gray-700'],
        ['Connect Pod', 'bg-purple-50 border-purple-200 text-purple-700'],
        ['KafkaConnector CRs', 'bg-teal-50 border-teal-200 text-teal-800'],
        ['Tasks', 'bg-gray-100 border-gray-300 text-gray-700'],
      ]),
      hoodResource('KafkaConnect', 'kafka-connect', 'Define o cluster Connect: build com plugins Debezium (PG, Oracle, JDBC), replicas e config', '5-kafka-connect/0-kafka-connect.yaml') +
      hoodResource('KafkaConnector', 'debezium-postgres', 'Source Connector: PostgreSQL CDC via pgoutput', '5-kafka-connect/1-pg-connector.yaml') +
      hoodResource('KafkaConnector', 'debezium-oracle', 'Source Connector: Oracle CDC via LogMiner', '5-kafka-connect/2-oracle-connector.yaml') +
      hoodResource('KafkaConnector', 'jdbc-sink-oracle-to-pg', 'Sink Connector: replica Oracle → PostgreSQL via JDBC', '5-kafka-connect/3-jdbc-sink.yaml') +
      hoodResource('NetworkPolicy', 'demo-ui-to-kafka-connect', 'Permite que o demo-ui acesse a API REST do Connect (porta 8083)', '6-demo-ui/0-demo-ui.yaml'),
      'O <strong>KafkaConnect CR</strong> instrui o Strimzi a construir uma imagem Docker customizada via S2I, incluindo os plugins Debezium (PostgreSQL, Oracle e JDBC Sink) como artefatos Maven. O annotation <code>strimzi.io/use-connector-resources: true</code> habilita o gerenciamento declarativo de conectores via <strong>KafkaConnector CRs</strong>. O Strimzi Entity Operator monitora esses CRs e cria/atualiza/remove os conectores no cluster Connect automaticamente.'
    )}

    ${confluentComparison(
      cfRow(
        'Fully-Managed Connectors — conectores como serviço SaaS',
        'KafkaConnect CR + KafkaConnector CRs — conectores auto-gerenciados via Strimzi',
        'good',
        'A tecnologia subjacente é a mesma: Apache Kafka Connect. A diferença é operacional — no Confluent, você cria conectores via UI/CLI e a infraestrutura é invisível. No Red Hat, você declara o <code>KafkaConnect</code> CR (com build dos plugins) e os <code>KafkaConnector</code> CRs. Isso dá mais controle, mas exige que a equipe de plataforma gerencie imagens, builds e monitoring.'
      ),
      'Em um ambiente Confluent típico, dezenas de conectores (CDC, Sinks, Sources) rodam como serviço totalmente gerenciado. Na migração, a equipe precisará montar o KafkaConnect com os plugins necessários e definir cada conector como CRD — um ganho para GitOps, mas com responsabilidade operacional adicional.'
    )}
  </div>`;
}

// ── Step 7: Schema Registry ──────────────────────────

function renderSchema() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Schema Registry</h2><p class="text-gray-500 mt-1">Red Hat build of Apicurio Registry para registro, versionamento e validação de schemas.</p></div>

    ${scenario('A <strong>TechMart</strong> tem múltiplas equipes produzindo e consumindo eventos: checkout, fulfillment, analytics, marketing. Sem um Schema Registry, cada equipe define seu próprio formato de dados — e quando alguém muda um campo, os consumidores quebram silenciosamente. O Apicurio Registry funciona como um <strong>contrato central</strong>: todos concordam sobre a estrutura dos dados, e qualquer mudança é versionada e validada antes de chegar à produção.')}

    ${card('Acesso Direto', `
      <div class="flex gap-3">
        ${CFG.apicurioUiUrl ? eLink(CFG.apicurioUiUrl, 'Abrir Apicurio UI') : '<span class="text-gray-400 text-sm">URL não configurada</span>'}
      </div>
    `)}

    ${card('Schemas Registrados', `
      <div id="r-artifacts"><div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Carregando schemas...</div></div>
    `)}

    ${card('Registrar Schema de Teste', `
      <p class="text-xs text-gray-500 mb-3">Registra um JSON Schema chamado <code class="bg-gray-100 px-1 rounded">demo-order</code> com a estrutura de um pedido.</p>
      ${pBtn('📝 Registrar Schema','registerSchema()')}
      ${res('r-register')}
    `)}

    ${tip('"O Apicurio Registry armazena e versiona schemas que definem a estrutura dos dados trafegados no Kafka. Producers e consumers podem validar mensagens contra o schema registrado, garantindo que alterações incompatíveis sejam detectadas antes de causar falhas em produção."')}

    ${underTheHood(
      hoodFlow([
        ['ApicurioRegistry3 CR', 'bg-purple-50 border-purple-200 text-purple-800'],
        ['Operator', 'bg-gray-100 border-gray-300 text-gray-700'],
        ['App Deployment', 'bg-purple-50 border-purple-200 text-purple-700'],
        ['KafkaSQL Storage', 'bg-red-50 border-red-200 text-red-700'],
        ['UI Deployment', 'bg-purple-50 border-purple-200 text-purple-700'],
      ]),
      hoodResource('ApicurioRegistry', 'apicurio-registry', 'CR que define o Apicurio Registry v3 com storage KafkaSQL', '2-apicurio/1-registry.yaml') +
      hoodResource('Deployment', 'apicurio-registry-app-deployment', 'Backend do Registry — API REST v3 para schemas e artifacts (gerenciado pelo operator)', '') +
      hoodResource('Deployment', 'apicurio-registry-ui-deployment', 'Frontend do Registry — UI web para navegação e gestão de schemas (gerenciado pelo operator)', '') +
      hoodResource('Route', 'apicurio-registry-api', 'Rota externa para a API REST do Registry', '2-apicurio/2-routes.yaml') +
      hoodResource('Route', 'apicurio-registry-ui', 'Rota externa para a UI do Registry', '2-apicurio/2-routes.yaml'),
      'O <strong>ApicurioRegistry3 CR</strong> instrui o Apicurio Operator a criar dois Deployments: o App (API backend) e o UI (frontend web). O storage utiliza <strong>KafkaSQL</strong>, que armazena schemas como mensagens em tópicos internos do cluster Kafka — sem necessidade de banco de dados externo. As <strong>Routes</strong> expõem a API (<code>/apis/registry/v3</code>) e a UI externamente.'
    )}

    ${confluentComparison(
      cfRow(
        'Confluent Schema Registry — schemas Avro/Protobuf, SaaS gerenciado',
        'Red Hat build of Apicurio Registry v3 — storage KafkaSQL, API REST v3',
        'strong',
        'Apicurio suporta Avro, Protobuf, JSON Schema e outros formatos. Possui regras de compatibilidade (BACKWARD, FORWARD, FULL), versionamento e serializers/deserializers compatíveis com o ecossistema Kafka. A migração exige mapear subjects/artifacts, formatos e regras de compatibilidade atuais.'
      ),
      'Em ambientes com forte governança de dados, o Schema Registry pode gerenciar centenas ou milhares de schemas vinculados à maioria dos tópicos. A migração dos schemas em si é viável — o ponto de atenção é garantir que serializers/deserializers das aplicações sejam compatíveis e que as políticas de compatibilidade sejam preservadas.'
    )}
  </div>`;
}

// ── Step 8: Data Contracts ───────────────────────────

function renderContracts() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Data Contracts</h2><p class="text-gray-500 mt-1">Demonstração de compatibilidade, metadata, políticas de evolução e falha controlada.</p></div>

    ${scenario('A equipe de checkout da <strong>TechMart</strong> precisa adicionar um campo <code>discount</code> ao schema de pedidos. Mas o serviço de fulfillment ainda não foi atualizado para ler esse campo. O <strong>Data Contract</strong> com política BACKWARD garante que essa evolução é <strong>segura</strong>: adicionar um campo opcional não quebra consumidores antigos. Porém, se alguém tentar <strong>remover</strong> um campo obrigatório como <code>price</code>, o Registry rejeita — protegendo os consumidores de uma mudança destrutiva.')}

    ${card('1. Definir Política de Compatibilidade', `
      <p class="text-xs text-gray-500 mb-3">Configura regra BACKWARD no artifact <code class="bg-gray-100 px-1 rounded">demo-order</code>: novas versões devem ser retrocompatíveis.</p>
      ${pBtn('🛡️ Definir BACKWARD','setCompatibility()')}
      ${res('r-compat')}
    `)}

    ${card('2. Evolução Compatível', `
      <p class="text-xs text-gray-500 mb-3">Adiciona campo opcional <code class="bg-gray-100 px-1 rounded">discount</code> ao schema — evolução retrocompatível.</p>
      ${pBtn('✅ Registrar Versão 2.0.0','evolveCompatible()')}
      ${res('r-evolve-ok')}
    `)}

    ${card('3. Evolução Incompatível (Falha Esperada)', `
      <p class="text-xs text-gray-500 mb-3">Remove campo obrigatório <code class="bg-gray-100 px-1 rounded">price</code> — deve ser rejeitada pela regra BACKWARD.</p>
      ${dBtn('❌ Tentar Versão Incompatível','evolveIncompatible()')}
      ${res('r-evolve-fail')}
    `)}

    ${card('4. Metadata e Labels', `
      <p class="text-xs text-gray-500 mb-3">Adiciona metadata descritivo e labels ao artifact.</p>
      ${sBtn('🏷️ Adicionar Metadata','addMetadata()')}
      ${res('r-metadata')}
    `)}

    ${tip('"Data Contracts vão além do schema: incluem metadata (quem é o owner, descrição, SLA), labels para categorização, e políticas de compatibilidade que impedem mudanças quebradoras. Quando tentamos registrar um schema incompatível, o Registry rejeita com HTTP 409 — isso é a governança em ação."')}

    ${underTheHood(
      hoodFlow([
        ['Artifact (demo-order)', 'bg-purple-50 border-purple-200 text-purple-800'],
        ['Compatibility Rule', 'bg-yellow-50 border-yellow-200 text-yellow-800'],
        ['Version Check', 'bg-gray-100 border-gray-300 text-gray-700'],
        ['Accept / Reject', 'bg-green-50 border-green-200 text-green-800'],
      ]),
      hoodResource('ApicurioRegistry', 'apicurio-registry', 'O mesmo Registry da página anterior — todas as APIs de Data Contracts são do Apicurio v3', '2-apicurio/1-registry.yaml'),
      'Data Contracts são implementados usando a <strong>API REST v3</strong> do Apicurio Registry. O fluxo é: (1) registrar o artifact com <code>POST /groups/default/artifacts</code>, (2) definir regra de compatibilidade com <code>POST /groups/default/artifacts/{id}/rules</code>, (3) ao registrar nova versão com <code>POST /groups/default/artifacts/{id}/versions</code>, o Registry valida contra a regra — se incompatível, retorna <strong>HTTP 409</strong>. Labels e metadata são gerenciados via <code>PUT /groups/default/artifacts/{id}</code>.'
    )}

    ${confluentComparison(
      cfRow(
        'Confluent Data Contracts — feature nativa integrada ao Schema Registry',
        'Apicurio Registry — schemas + metadata + labels + regras de compatibilidade',
        'partial',
        'O Confluent trata Data Contracts como uma feature de primeira classe (schema + metadata + migration rules + quality rules). O Apicurio oferece os building blocks (schemas, versionamento, compatibilidade, metadata, labels), mas a composição dessas peças em um "contrato" é responsabilidade da aplicação e da governança.'
      ),
      'Em ambientes Confluent com forte uso de Data Contracts, a maioria dos tópicos pode ter validação ativa. Para qualquer migração, é essencial mapear <strong>quais capabilities</strong> de Data Contracts são efetivamente usadas: apenas schema + compatibilidade? Ou também quality rules, migration rules, metadata avançado? Isso determina o gap real entre Confluent e Apicurio.'
    )}
  </div>`;
}

// ── Step 9: Summary ──────────────────────────────────

function renderSummary() {
  const items = [
    ['📨','Kafka Cluster','3 brokers KRaft, tópicos, partições e replicação'],
    ['📤','Producer / Consumer','Aplicações Quarkus trocando eventos via Kafka'],
    ['🐘','CDC PostgreSQL','Debezium capturando INSERT, UPDATE, DELETE em tempo real'],
    ['🔶','CDC Oracle','LogMiner + Debezium publicando mudanças no Kafka'],
    ['⬇️','Sink Connector','JDBC Sink replicando Oracle → PostgreSQL via Kafka'],
    ['🔌','Kafka Connect','Framework de integração gerenciando 3 conectores'],
    ['📋','Schema Registry','Apicurio Registry com versionamento e validação'],
    ['🛡️','Data Contracts','Compatibilidade BACKWARD, metadata, labels e falha controlada'],
  ];
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Resumo da Demonstração</h2><p class="text-gray-500 mt-1">Tudo que foi demonstrado nesta demo.</p></div>

    ${scenario('Com esta arquitetura, a <strong>TechMart</strong> conseguiu: substituir o batch noturno por <strong>replicação em tempo real</strong> entre Oracle e PostgreSQL; desacoplar o checkout dos serviços de fulfillment, estoque e notificação via <strong>pub/sub</strong>; capturar mudanças em ambos os bancos sem alterar as aplicações existentes (<strong>CDC</strong>); e garantir que a evolução dos formatos de dados seja <strong>governada e segura</strong> via Schema Registry e Data Contracts. Tudo isso rodando sobre o <strong>stack Red Hat</strong>, com suporte empresarial e integração nativa com OpenShift.')}

    ${card('Conceitos Demonstrados', `<div class="space-y-3">${items.map(([e, t, d]) =>
      `<div class="flex items-start gap-3 p-3 rounded-lg bg-gray-50"><span class="text-xl">${e}</span><div><p class="font-medium text-sm">${t}</p><p class="text-xs text-gray-500">${d}</p></div><span class="ml-auto text-green-500 text-lg">✓</span></div>`
    ).join('')}</div>`)}

    ${card('Links Externos', `<div class="flex flex-wrap gap-3">
      ${CFG.argocdUrl ? eLink(CFG.argocdUrl, 'ArgoCD') : ''}
      ${CFG.apicurioUiUrl ? eLink(CFG.apicurioUiUrl, 'Apicurio UI') : ''}
    </div>`)}

    ${tip('"Demonstramos o ciclo completo de uma arquitetura de streaming Red Hat: desde a publicação de eventos por uma aplicação, passando pela captura de mudanças em bancos de dados com Debezium, integração com destinos externos via Sink Connector, até a governança de schemas e Data Contracts com o Apicurio Registry. Todos os componentes são open source e suportados pela Red Hat."')}
  </div>`;
}

// ── Action functions ─────────────────────────────────

async function sendOrder() {
  showSpin('r-send');
  try {
    const body = {
      customerName: $('o-name').value,
      product: $('o-prod').value,
      quantity: parseInt($('o-qty').value) || 1,
      price: parseFloat($('o-price').value) || 0,
    };
    const data = await api('/api/producer/orders', { method: 'POST', body: JSON.stringify(body) });
    showOk('r-send', data);
    loadOrders(true);
  } catch (e) { showErr('r-send', e.message); }
}

async function loadOrders(silent = false) {
  if (!silent) showSpin('r-orders');
  try {
    const data = await api('/api/consumer/orders');
    const arr = Array.isArray(data) ? data : [];
    const items = arr.map(o => {
      const ts = o.timestamp ? new Date(o.timestamp).getTime() : trackSeen('orders', o.orderId || o.customerName);
      return { ts, row: [o.orderId || '-', o.customerName, o.product, o.quantity, o.price, agoBadge(ts)] };
    });
    items.sort((a, b) => b.ts - a.ts);
    showTable('r-orders', ['ID', 'Cliente', 'Produto', 'Qtd', 'Preço', ''], items.map(i => i.row), [5]);
    setHtml('r-count', `${items.length} pedido(s)`);
  } catch (e) {
    if (!silent) {
      if (/fetch|network|ECONNRE|ETIMEDOUT|socket/i.test(e.message)) {
        setHtml('r-orders', '<div class="flex items-center gap-2 text-gray-400 text-sm py-3"><span class="spinner"></span> Aguardando consumer...</div>');
        setHtml('r-count', '');
      } else {
        showErr('r-orders', e.message);
      }
    }
  }
}

async function listTopics() {
  try {
    const data = await api('/api/kafka/topics');
    const html = data.map(t => `<button onclick="document.getElementById('t-name').value='${t}';describeTopic()" class="block w-full text-left px-3 py-2 text-sm rounded hover:bg-gray-100 font-mono">${esc(t)}</button>`).join('');
    setHtml('r-topics', `<div class="border rounded-lg divide-y max-h-64 overflow-y-auto">${html}</div>`);
  } catch (e) { showErr('r-topics', e.message); }
}

async function describeTopic() {
  const el = $('t-name');
  const name = el ? el.value.trim() : '';
  if (!name) return;
  try {
    const d = await api(`/api/kafka/topics/${encodeURIComponent(name)}`);
    const parts = (d.partitions || []).map(p => [p.partitionId, p.leader, (p.replicas||[]).map(r=>r.nodeId).join(','), (p.isr||[]).map(r=>r.nodeId).join(',')]);
    let h = `<p class="text-sm mb-2"><strong>Tópico:</strong> ${esc(name)} &nbsp; <strong>Partições:</strong> ${parts.length}</p>`;
    h += `<div class="overflow-x-auto rounded-lg border"><table class="min-w-full divide-y divide-gray-200"><thead class="bg-gray-50"><tr><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">Partição</th><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">Líder</th><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">Réplicas</th><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">ISR</th></tr></thead><tbody class="divide-y">${parts.map(r => `<tr>${r.map(c => `<td class="px-3 py-2 text-sm">${c}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
    setHtml('r-topic-detail', h);
  } catch (e) { showErr('r-topic-detail', e.message); }
}

// ── CDC helpers ──────────────────────────────────────

const CDC_TOPIC = { pg: 'debezium.public.customers', ora: 'oracle.DEBEZIUM.CUSTOMERS' };

function cdcRefreshAfterWrite(base) {
  cdcTable(base, true);
  setTimeout(() => cdcEvents(base, CDC_TOPIC[base], true), 3000);
}

async function cdcInsert(base) {
  const rid = `r-${base}-insert`;
  showSpin(rid);
  try {
    const name = $(`${base}-name`).value, email = $(`${base}-email`).value, city = $(`${base}-city`).value;
    const sql = base === 'pg'
      ? `INSERT INTO customers (name, email, city) VALUES ('${name}', '${email}', '${city}')`
      : `INSERT INTO CUSTOMERS (NAME, EMAIL, CITY) VALUES ('${name}', '${email}', '${city}')`;
    const data = await api(`/api/${base === 'pg' ? 'pg' : 'oracle'}/execute`, { method: 'POST', body: JSON.stringify({ sql }) });
    showOk(rid, { status: 'OK', ...data, sql });
    cdcRefreshAfterWrite(base);
  } catch (e) { showErr(rid, e.message); }
}

async function cdcUpdate(base) {
  const rid = `r-${base}-mod`;
  showSpin(rid);
  try {
    const id = $(`${base}-upd-id`).value, field = $(`${base}-upd-field`).value, val = $(`${base}-upd-val`).value;
    const sql = base === 'pg'
      ? `UPDATE customers SET ${field} = '${val}' WHERE id = ${id}`
      : `UPDATE CUSTOMERS SET ${field.toUpperCase()} = '${val}' WHERE ID = ${id}`;
    const data = await api(`/api/${base === 'pg' ? 'pg' : 'oracle'}/execute`, { method: 'POST', body: JSON.stringify({ sql }) });
    showOk(rid, { status: 'OK', ...data, sql });
    cdcRefreshAfterWrite(base);
  } catch (e) { showErr(rid, e.message); }
}

async function cdcDelete(base) {
  const rid = `r-${base}-mod`;
  showSpin(rid);
  try {
    const id = $(`${base}-upd-id`).value;
    const sql = base === 'pg' ? `DELETE FROM customers WHERE id = ${id}` : `DELETE FROM CUSTOMERS WHERE ID = ${id}`;
    const data = await api(`/api/${base === 'pg' ? 'pg' : 'oracle'}/execute`, { method: 'POST', body: JSON.stringify({ sql }) });
    showOk(rid, { status: 'OK', ...data, sql });
    cdcRefreshAfterWrite(base);
  } catch (e) { showErr(rid, e.message); }
}

async function cdcTable(base, silent = false) {
  const rid = `r-${base}-table`;
  if (!$(rid)) return;
  try {
    const data = await api(`/api/${base === 'pg' ? 'pg' : 'oracle'}/customers`);
    const cols = base === 'pg' ? ['id','name','email','city'] : ['ID','NAME','EMAIL','CITY'];
    const ns = `${base}-cust`;
    const items = data.map(r => {
      const ts = (base === 'pg' && r.created_at) ? new Date(r.created_at).getTime() : trackSeen(ns, r[cols[0]]);
      return { ts, row: [...cols.map(k => r[k]), agoBadge(ts)] };
    });
    items.sort((a, b) => b.ts - a.ts);
    showTable(rid, [...cols, ''], items.map(i => i.row), [cols.length]);
  } catch (e) { if (!silent) showErr(rid, e.message); }
}

async function cdcEvents(base, topic, silent = false) {
  const rid = `r-${base}-cdc`;
  if (!$(rid)) return;
  if (!silent) showSpin(rid);
  try {
    const msgs = await api(`/api/kafka/consume/${encodeURIComponent(topic)}?limit=5`);
    if (!msgs.length) return setHtml(rid, '<p class="text-gray-500 italic text-sm py-2">Nenhum evento encontrado (o consumer pode levar alguns segundos).</p>');
    const html = msgs.map(m => {
      let val = m.value;
      let isTombstone = (val == null);
      if (!isTombstone) { try { val = JSON.stringify(JSON.parse(val), null, 2); } catch (_) {} }
      const ts = m.timestamp ? parseInt(m.timestamp, 10) : trackSeen(`${base}-evt`, m.offset);
      const body = isTombstone
        ? '<div class="px-3 py-2 text-xs text-gray-400 italic bg-gray-50">tombstone (registro removido)</div>'
        : `<pre class="code text-xs" style="border-radius:0">${esc(val)}</pre>`;
      return `<div class="mb-3 border rounded-lg overflow-hidden">
        <div class="bg-gray-100 px-3 py-1.5 text-xs text-gray-600 flex items-center gap-4"><span>Partição: ${m.partition}</span><span>Offset: ${m.offset}</span><span>Key: ${esc(m.key || 'null')}</span>${isTombstone ? '<span class="text-red-400 font-medium">DELETE</span>' : ''}<span class="ml-auto">${agoBadge(ts)}</span></div>
        ${body}
      </div>`;
    }).join('');
    setHtml(rid, html);
  } catch (e) { if (!silent) showErr(rid, e.message); }
}

// ── Sink helpers ─────────────────────────────────────

async function sinkSource(silent = false) {
  const rid = 'r-sink-src';
  if (!$(rid)) return;
  try {
    const data = await api('/api/oracle/customers');
    const items = data.map(r => {
      const ts = trackSeen('sink-src', r.ID);
      return { ts, row: [r.ID, r.NAME, r.EMAIL, r.CITY, agoBadge(ts)] };
    });
    items.sort((a, b) => b.ts - a.ts);
    showTable(rid, ['ID','Nome','Email','Cidade',''], items.map(i => i.row), [4]);
  } catch (e) { if (!silent) showErr(rid, e.message); }
}

async function sinkDest(silent = false) {
  const rid = 'r-sink-dst';
  if (!$(rid)) return;
  try {
    const data = await api('/api/pg/oracle-customers');
    const keys = data.length ? Object.keys(data[0]) : [];
    const badgeCol = keys.length;
    const items = data.map(r => {
      const id = r[keys[0]] || JSON.stringify(keys.map(k => r[k]));
      const ts = trackSeen('sink-dst', id);
      return { ts, row: [...keys.map(k => r[k]), agoBadge(ts)] };
    });
    items.sort((a, b) => b.ts - a.ts);
    showTable(rid, [...keys, ''], items.map(i => i.row), [badgeCol]);
  } catch (e) { if (!silent) showErr(rid, e.message); }
}

async function sinkStatus(silent = false) {
  const rid = 'r-sink-status';
  if (!$(rid)) return;
  try {
    const data = await api('/api/kafka-connect/connectors/jdbc-sink-oracle-to-pg/status');
    showOk(rid, data);
  } catch (e) { if (!silent) showErr(rid, e.message); }
}

// ── Connect helpers ──────────────────────────────────

async function listConnectors(silent = false) {
  const rid = 'r-connectors';
  if (!$(rid)) return;
  try {
    const data = await api('/api/kafka-connect/connectors');
    const names = Object.keys(data);
    if (!names.length) return setHtml(rid, '<p class="text-gray-500 italic text-sm">Nenhum connector encontrado.</p>');
    const html = names.map(name => {
      const info = data[name];
      const st = info.status || {};
      const connState = st.connector?.state || 'UNKNOWN';
      const tasks = (st.tasks || []).map(t => t.state).join(', ') || '-';
      const stColor = connState === 'RUNNING' ? 'green' : connState === 'PAUSED' ? 'yellow' : 'red';
      return `<div class="flex items-center justify-between p-4 rounded-lg border hover:bg-gray-50">
        <div>
          <p class="font-medium text-sm">${esc(name)}</p>
          <p class="text-xs text-gray-500 mt-0.5">Type: ${esc(st.type || '-')} · Tasks: ${esc(tasks)}</p>
        </div>
        <span class="inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full bg-${stColor}-100 text-${stColor}-700"><span class="w-2 h-2 rounded-full bg-${stColor}-500"></span>${connState}</span>
      </div>`;
    }).join('');
    setHtml(rid, `<div class="space-y-2">${html}</div>`);
  } catch (e) { if (!silent) showErr(rid, e.message); }
}

// ── Schema / Data Contract helpers ───────────────────

const ORDER_SCHEMA_V1 = {
  type: 'object',
  additionalProperties: false,
  properties: {
    orderId: { type: 'string', description: 'Identificador único do pedido' },
    customerName: { type: 'string', description: 'Nome do cliente' },
    product: { type: 'string', description: 'Produto adquirido' },
    quantity: { type: 'integer', description: 'Quantidade' },
    price: { type: 'number', description: 'Preço unitário' },
    timestamp: { type: 'string', format: 'date-time' },
  },
  required: ['orderId', 'customerName', 'product', 'quantity', 'price'],
};

const ORDER_SCHEMA_V2 = {
  type: 'object',
  additionalProperties: false,
  properties: {
    ...ORDER_SCHEMA_V1.properties,
    discount: { type: 'number', description: 'Desconto aplicado (opcional)' },
  },
  required: ['orderId', 'customerName', 'product', 'quantity', 'price'],
};

const ORDER_SCHEMA_BAD = {
  type: 'object',
  additionalProperties: false,
  properties: {
    orderId: { type: 'string' },
    customerName: { type: 'string' },
    product: { type: 'string' },
    quantity: { type: 'integer' },
  },
  required: ['orderId', 'customerName', 'product', 'quantity'],
};

async function listArtifacts() {
  const rid = 'r-artifacts';
  if (!$(rid)) return;
  try {
    const data = await api('/api/apicurio/search/artifacts?limit=50');
    const artifacts = data.artifacts || [];
    if (!artifacts.length) return setHtml(rid, '<p class="text-gray-500 italic text-sm">Nenhum artifact registrado.</p>');
    const items = artifacts.map(a => {
      const ts = a.createdOn ? new Date(a.createdOn).getTime() : trackSeen('artifacts', a.artifactId);
      return { ts, row: [a.artifactId, a.artifactType, a.name || '-', agoBadge(ts)] };
    });
    items.sort((a, b) => b.ts - a.ts);
    showTable(rid, ['Artifact ID', 'Tipo', 'Nome', ''], items.map(i => i.row), [3]);
  } catch (e) { showErr(rid, e.message); }
}

async function registerSchema() {
  showSpin('r-register');
  try {
    await fetch('/api/apicurio/groups/default/artifacts/demo-order', { method: 'DELETE' }).catch(() => {});
    const body = {
      artifactId: 'demo-order',
      artifactType: 'JSON',
      name: 'Order Schema',
      description: 'Schema do pedido para demonstração de Data Contracts',
      firstVersion: {
        version: '1.0.0',
        content: { content: JSON.stringify(ORDER_SCHEMA_V1), contentType: 'application/json' },
      },
    };
    const data = await api('/api/apicurio/groups/default/artifacts', { method: 'POST', body: JSON.stringify(body) });
    showOk('r-register', { status: 'Schema registrado com sucesso (estado limpo)', artifact: data });
    listArtifacts();
  } catch (e) { showErr('r-register', e.message); }
}

async function setCompatibility() {
  showSpin('r-compat');
  try {
    await api('/api/apicurio/groups/default/artifacts/demo-order/rules', {
      method: 'POST',
      body: JSON.stringify({ ruleType: 'COMPATIBILITY', config: 'BACKWARD' }),
    });
    showOk('r-compat', { status: 'Regra BACKWARD configurada com sucesso', rule: 'COMPATIBILITY', config: 'BACKWARD' });
  } catch (e) { showErr('r-compat', e.message); }
}

async function evolveCompatible() {
  showSpin('r-evolve-ok');
  try {
    const body = {
      version: '2.0.0',
      content: { content: JSON.stringify(ORDER_SCHEMA_V2), contentType: 'application/json' },
    };
    const data = await api('/api/apicurio/groups/default/artifacts/demo-order/versions', { method: 'POST', body: JSON.stringify(body) });
    setHtml('r-evolve-ok', `<div class="bg-green-50 border border-green-300 text-green-800 rounded-lg p-4 text-sm">
      <strong>✅ Sucesso!</strong> Versão 2.0.0 registrada. O campo opcional <code>discount</code> foi adicionado sem quebrar compatibilidade BACKWARD.
      <pre class="code mt-3">${esc(JSON.stringify(data, null, 2))}</pre>
    </div>`);
  } catch (e) { showErr('r-evolve-ok', e.message); }
}

async function evolveIncompatible() {
  showSpin('r-evolve-fail');
  try {
    const body = {
      version: '3.0.0-bad',
      content: { content: JSON.stringify(ORDER_SCHEMA_BAD), contentType: 'application/json' },
    };
    const data = await api('/api/apicurio/groups/default/artifacts/demo-order/versions', { method: 'POST', body: JSON.stringify(body) });
    setHtml('r-evolve-fail', `<div class="bg-yellow-50 border border-yellow-300 text-yellow-800 rounded-lg p-4 text-sm">
      <strong>⚠️ Inesperado:</strong> a versão foi aceita. Verifique se a regra de compatibilidade está configurada.
      <pre class="code mt-3">${esc(JSON.stringify(data, null, 2))}</pre>
    </div>`);
  } catch (e) {
    setHtml('r-evolve-fail', `<div class="bg-red-50 border border-red-300 text-red-800 rounded-lg p-4 text-sm">
      <strong>🛡️ Rejeitado (esperado)!</strong> O Registry recusou a versão incompatível. A remoção do campo obrigatório <code>price</code> viola a regra BACKWARD.
      <p class="mt-2 text-xs font-mono text-red-600">${esc(e.message)}</p>
    </div>`);
  }
}

async function addMetadata() {
  showSpin('r-metadata');
  try {
    const body = {
      name: 'Order Schema',
      description: 'Schema do pedido — Data Contract gerenciado',
      labels: { domain: 'orders', owner: 'team-streaming', sla: 'tier-1', classification: 'internal' },
    };
    await api('/api/apicurio/groups/default/artifacts/demo-order', { method: 'PUT', body: JSON.stringify(body) });
    showOk('r-metadata', { status: 'Metadata atualizado com sucesso', labels: body.labels, description: body.description });
  } catch (e) { showErr('r-metadata', e.message); }
}

// ── Navigation ───────────────────────────────────────

function goTo(i) {
  stopPolls();
  step = Math.max(0, Math.min(STEPS.length - 1, i));
  $('step-content').innerHTML = RENDERS[step]();
  renderNav();
  $('step-indicator').textContent = `${step + 1} / ${STEPS.length}`;
  $('btn-prev').style.visibility = step === 0 ? 'hidden' : 'visible';
  $('btn-next').textContent = step === STEPS.length - 1 ? 'Concluir ✓' : 'Próximo →';
  $('step-content').parentElement.scrollTop = 0;

  const init = STEP_INIT[step];
  if (init) setTimeout(init, 80);
}

function nextStep() { if (step < STEPS.length - 1) goTo(step + 1); }
function prevStep() { if (step > 0) goTo(step - 1); }

function renderNav() {
  $('step-nav').innerHTML = STEPS.map((s, i) => {
    const active = i === step;
    return `<button onclick="goTo(${i})" class="step-item w-full flex items-center gap-3 px-4 py-2.5 text-left ${active ? 'step-active' : ''}">
      <span class="w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0 ${active ? 'bg-rh-red text-white' : 'bg-gray-700 text-gray-400'}">${i + 1}</span>
      <div class="min-w-0">
        <p class="text-sm font-medium truncate ${active ? 'text-white' : 'text-gray-300'}">${s.t}</p>
        <p class="text-xs truncate ${active ? 'text-gray-400' : 'text-gray-500'}">${s.s}</p>
      </div>
    </button>`;
  }).join('');
}

// ── Reset ────────────────────────────────────────────

async function resetDemo() {
  if (!confirm('Restaurar a demo ao estado inicial?\n\nIsso vai resetar os bancos de dados (PostgreSQL e Oracle) com a carga inicial e remover artefatos do Apicurio Registry.')) return;

  const overlay = document.createElement('div');
  overlay.className = 'fixed inset-0 bg-black/50 flex items-center justify-center z-50';
  overlay.innerHTML = '<div class="bg-white rounded-xl shadow-2xl p-8 max-w-md mx-4 text-center"><div class="spinner mb-4" style="width:2rem;height:2rem;border-width:3px"></div><p class="text-sm text-gray-700 font-medium">Restaurando estado inicial...</p></div>';
  document.body.appendChild(overlay);

  try {
    const data = await api('/api/reset', { method: 'POST' });
    Object.keys(_seen).forEach(k => delete _seen[k]);
    overlay.innerHTML = `<div class="bg-white rounded-xl shadow-2xl p-8 max-w-md mx-4">
      <p class="text-green-700 font-bold text-base mb-3">Estado restaurado</p>
      <ul class="text-sm text-gray-600 space-y-1">${(data.actions || []).map(a => `<li class="flex items-start gap-2"><span class="text-green-500 mt-0.5">✓</span>${a}</li>`).join('')}</ul>
      <button onclick="this.closest('.fixed').remove();goTo(step)" class="mt-5 w-full bg-rh-red hover:bg-red-700 text-white text-sm font-medium rounded-lg px-4 py-2 transition-colors">Fechar</button>
    </div>`;
  } catch (e) {
    overlay.innerHTML = `<div class="bg-white rounded-xl shadow-2xl p-8 max-w-md mx-4">
      <p class="text-red-700 font-bold text-base mb-2">Erro ao restaurar</p>
      <p class="text-sm text-gray-600">${e.message}</p>
      <button onclick="this.closest('.fixed').remove()" class="mt-4 w-full bg-gray-200 hover:bg-gray-300 text-gray-700 text-sm font-medium rounded-lg px-4 py-2 transition-colors">Fechar</button>
    </div>`;
  }
}

// ── Init ─────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  try { CFG = await api('/api/config'); } catch (_) { CFG = {}; }
  goTo(0);
});
