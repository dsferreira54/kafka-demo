/* Kafka Streaming POC — Demo UI */

let CFG = {};
let step = 0;

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

function showTable(id, headers, rows) {
  if (!rows || !rows.length) return setHtml(id, '<p class="text-gray-500 italic text-sm py-2">Nenhum dado encontrado.</p>');
  const ths = headers.map(h => `<th class="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">${h}</th>`).join('');
  const trs = rows.map(r => `<tr class="hover:bg-gray-50">${r.map(c => `<td class="px-3 py-2 text-sm whitespace-nowrap">${c != null ? esc(String(c)) : '<span class="text-gray-300">null</span>'}</td>`).join('')}</tr>`).join('');
  setHtml(id, `<div class="overflow-x-auto mt-3 rounded-lg border"><table class="min-w-full divide-y divide-gray-200"><thead class="bg-gray-50"><tr>${ths}</tr></thead><tbody class="divide-y divide-gray-200">${trs}</tbody></table></div>`);
}

function card(title, body, extra = '') {
  return `<div class="bg-white rounded-xl shadow-md p-6 mb-5 ${extra}">${title ? `<h3 class="text-base font-semibold text-gray-800 mb-4">${title}</h3>` : ''}${body}</div>`;
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
  return `<div class="bg-amber-50 border-l-4 border-amber-400 p-4 rounded-r-lg mt-4"><div class="flex gap-3"><span class="text-lg">💡</span><div><p class="text-xs font-bold text-amber-800 mb-1 uppercase tracking-wide">O que explicar</p><p class="text-sm text-amber-700 leading-relaxed">${text}</p></div></div></div>`;
}

function pBtn(text, fn) { return `<button onclick="${fn}" class="bg-rh-red hover:bg-red-700 text-white text-sm font-medium rounded-lg px-4 py-2 transition-colors">${text}</button>`; }
function sBtn(text, fn) { return `<button onclick="${fn}" class="bg-gray-100 hover:bg-gray-200 text-gray-700 text-sm font-medium rounded-lg px-4 py-2 border border-gray-200 transition-colors">${text}</button>`; }
function dBtn(text, fn) { return `<button onclick="${fn}" class="bg-red-50 hover:bg-red-100 text-red-700 text-sm font-medium rounded-lg px-4 py-2 border border-red-300 transition-colors">${text}</button>`; }
function eLink(url, text) { return `<a href="${url}" target="_blank" class="inline-flex items-center gap-1.5 bg-teal-600 hover:bg-teal-700 text-white text-sm font-medium rounded-lg px-4 py-2 transition-colors">${text} ↗</a>`; }
function inp(id, label, ph, val = '') { return `<div><label class="block text-xs font-medium text-gray-600 mb-1">${label}</label><input id="${id}" placeholder="${ph}" value="${val}" class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-2 focus:ring-red-500 focus:border-red-500 outline-none"></div>`; }
function res(id) { return `<div id="${id}" class="hidden mt-3"></div>`; }

function compCard(name, desc, emoji, c) {
  return `<div class="flex items-center gap-3 p-3 rounded-lg bg-${c}-50 border border-${c}-200"><span class="text-xl">${emoji}</span><div><p class="font-medium text-sm">${name}</p><p class="text-xs text-gray-500">${desc}</p></div></div>`;
}

// ── Step 0: Overview ─────────────────────────────────

function renderOverview() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Kafka Streaming POC</h2><p class="text-gray-500 mt-1">Prova de conceito de uma arquitetura de streaming baseada no stack Red Hat: Apache Kafka, CDC com Debezium, Schema Registry com Apicurio e Data Contracts.</p></div>

    ${card('Arquitetura', `<div class="bg-gray-50 rounded-lg p-5 font-mono text-xs overflow-x-auto"><pre class="text-center leading-relaxed text-gray-700">
┌───────────┐          ┌─────────────────┐          ┌───────────┐
│ Producer  │─────────▶│   Apache Kafka   │◀─────────│ Consumer  │
│ (Quarkus) │          │   3 brokers KRaft │          │ (Quarkus) │
└───────────┘          └────────┬────────┘          └───────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
     ┌────────▼────────┐  ┌────▼─────┐  ┌────────▼────────┐
     │  Debezium CDC   │  │  JDBC    │  │  Debezium CDC   │
     │  (PostgreSQL)   │  │  Sink    │  │  (Oracle)       │
     └────────┬────────┘  └────┬─────┘  └────────┬────────┘
              │                │                 │
     ┌────────▼────────┐  ┌────▼─────┐  ┌────────▼────────┐
     │  PostgreSQL 12  │◀─┤ destino  │  │ Oracle 23c Free │
     └─────────────────┘  └──────────┘  └─────────────────┘

                    ┌─────────────────┐
                    │ Apicurio Regis. │
                    │ (Schema/Contrat)│
                    └─────────────────┘</pre></div>`)}

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

    ${tip('"Esta POC demonstra como o stack Red Hat pode substituir uma plataforma Confluent. Vamos percorrer cada componente: pub/sub básico, CDC de PostgreSQL e Oracle, Sink Connector, Schema Registry e Data Contracts."')}
  </div>`;
}

// ── Step 1: Kafka Basics ─────────────────────────────

function renderKafka() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Kafka: Produzir e Consumir</h2><p class="text-gray-500 mt-1">Demonstração do fluxo básico de mensageria: publicar um pedido via Producer e consumi-lo via Consumer.</p></div>

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

    ${card('Pedidos Consumidos', `
      <div class="flex items-center gap-3 mb-1">${sBtn('📥 Carregar Pedidos','loadOrders()')}<span id="r-count" class="text-xs text-gray-400"></span></div>
      ${res('r-orders')}
    `)}

    ${tip('"O Producer é uma aplicação Quarkus que recebe pedidos via REST e publica no tópico <code>orders</code> do Kafka usando SmallRye Reactive Messaging. O Consumer consome do mesmo tópico e expõe via REST. Isso demonstra o padrão pub/sub: publisher → broker → subscriber."')}
  </div>`;
}

// ── Step 2: Topics ───────────────────────────────────

function renderTopics() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Tópicos e Partições</h2><p class="text-gray-500 mt-1">Explorar os tópicos criados no cluster Kafka, incluindo tópicos de aplicação e tópicos de CDC.</p></div>

    ${card('Tópicos do Cluster', `
      <div class="flex gap-2 mb-2">${pBtn('📋 Listar Tópicos','listTopics()')}</div>
      ${res('r-topics')}
    `)}

    ${card('Detalhes do Tópico', `
      <div class="flex gap-2 items-end mb-2">
        ${inp('t-name','Nome do Tópico','orders','orders')}
        <div class="pb-0.5">${sBtn('🔍 Detalhar','describeTopic()')}</div>
      </div>
      ${res('r-topic-detail')}
    `)}

    ${tip('"Cada tópico é dividido em partições. As partições permitem paralelismo no consumo e distribuição de carga entre brokers. Mensagens com a mesma chave vão para a mesma partição, garantindo ordenação por chave. O fator de replicação define quantas cópias existem para alta disponibilidade."')}
  </div>`;
}

// ── Generic CDC renderer ─────────────────────────────

function cdcStep(opts) {
  const { title, desc, dbLabel, dbEmoji, dbColor, apiBase, cdcTopic, tipText, extraButtons } = opts;
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">${title}</h2><p class="text-gray-500 mt-1">${desc}</p></div>

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

    ${card('Dados Atuais', `
      ${sBtn('🔄 Consultar Tabela',"cdcTable('" + apiBase + "')")}
      ${extraButtons || ''}
      ${res(`r-${apiBase}-table`)}
    `)}

    ${card('Eventos CDC no Kafka', `
      <p class="text-xs text-gray-500 mb-3">Tópico: <code class="bg-gray-100 px-1.5 py-0.5 rounded">${cdcTopic}</code></p>
      ${sBtn('📡 Ver Eventos CDC',"cdcEvents('" + apiBase + "','" + cdcTopic + "')")}
      ${res(`r-${apiBase}-cdc`)}
    `)}

    ${tip(tipText)}
  </div>`;
}

// ── Step 3: PG CDC ───────────────────────────────────

function renderPgCDC() {
  return cdcStep({
    title: 'CDC PostgreSQL',
    desc: 'Captura de mudanças no PostgreSQL via Debezium. Cada INSERT, UPDATE ou DELETE gera um evento no Kafka.',
    dbLabel: 'PostgreSQL', dbEmoji: '🐘', dbColor: 'indigo',
    apiBase: 'pg', cdcTopic: 'debezium.public.customers',
    tipText: '"O Debezium usa logical replication do PostgreSQL (pgoutput plugin) para capturar mudanças em tempo real. Cada operação no banco gera um evento com o tipo de operação (c=create, u=update, d=delete), os dados antes e depois, e metadados da origem."',
  });
}

// ── Step 4: Oracle CDC ───────────────────────────────

function renderOraCDC() {
  return cdcStep({
    title: 'CDC Oracle',
    desc: 'Captura de mudanças no Oracle Database 23c via Debezium com LogMiner.',
    dbLabel: 'Oracle', dbEmoji: '🔶', dbColor: 'amber',
    apiBase: 'ora', cdcTopic: 'oracle.DEBEZIUM.CUSTOMERS',
    tipText: '"O Debezium usa Oracle LogMiner para capturar mudanças, sem necessidade de triggers ou polling. O LogMiner lê os redo logs do banco e extrai as operações DML. É a mesma tecnologia usada pelo CDC em produção, mas aqui com Oracle 23c Free."',
  });
}

// ── Step 5: Sink Connector ───────────────────────────

function renderSink() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Sink Connector</h2><p class="text-gray-500 mt-1">Dados fluem do Oracle via CDC para o Kafka e são escritos automaticamente no PostgreSQL pelo JDBC Sink Connector.</p></div>

    ${flowDiagram([['Oracle','🔶','bg-amber-50 border-amber-200'],['Debezium','🔄','bg-green-50 border-green-200'],['Kafka','📨','bg-red-50 border-red-200'],['JDBC Sink','⬇️','bg-teal-50 border-teal-200'],['PostgreSQL','🐘','bg-indigo-50 border-indigo-200']])}

    ${card('Dados de Origem (Oracle → CUSTOMERS)', `
      ${sBtn('🔶 Consultar Oracle','sinkSource()')}
      ${res('r-sink-src')}
    `)}

    ${card('Dados de Destino (PostgreSQL → oracle_customers)', `
      ${sBtn('🐘 Consultar PostgreSQL','sinkDest()')}
      ${res('r-sink-dst')}
    `)}

    ${card('Status do Connector', `
      ${sBtn('📊 Ver Status','sinkStatus()')}
      ${res('r-sink-status')}
    `)}

    ${tip('"O JDBC Sink Connector do Debezium consome eventos CDC do tópico Oracle e os replica automaticamente numa tabela PostgreSQL. Isso demonstra o conceito de Sink Connector, análogo ao fluxo Kafka → Google Cloud Storage do ambiente real. O modo upsert garante idempotência: inserções e atualizações são tratadas corretamente."')}
  </div>`;
}

// ── Step 6: Kafka Connect ────────────────────────────

function renderConnect() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Kafka Connect</h2><p class="text-gray-500 mt-1">Visão operacional do cluster Kafka Connect e dos conectores configurados.</p></div>

    ${flowDiagram([['Kafka Connect','🔌','bg-purple-50 border-purple-200'],['Connectors','🔗','bg-teal-50 border-teal-200'],['Tasks','⚙️','bg-gray-100 border-gray-300']])}

    ${card('Conectores', `
      ${pBtn('📋 Listar Conectores','listConnectors()')}
      ${res('r-connectors')}
    `)}

    ${tip('"Kafka Connect é o framework de integração do Kafka. Ele gerencia Source Connectors (que trazem dados para o Kafka) e Sink Connectors (que levam dados do Kafka para fora). Cada connector pode ter múltiplas tasks executando em paralelo. O Strimzi operator gerencia o lifecycle dos connectors via KafkaConnector CRDs."')}
  </div>`;
}

// ── Step 7: Schema Registry ──────────────────────────

function renderSchema() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Schema Registry</h2><p class="text-gray-500 mt-1">Red Hat build of Apicurio Registry para registro, versionamento e validação de schemas.</p></div>

    ${card('Acesso Direto', `
      <div class="flex gap-3">
        ${CFG.apicurioUiUrl ? eLink(CFG.apicurioUiUrl, 'Abrir Apicurio UI') : '<span class="text-gray-400 text-sm">URL não configurada</span>'}
      </div>
    `)}

    ${card('Schemas Registrados', `
      ${sBtn('📋 Listar Artifacts','listArtifacts()')}
      ${res('r-artifacts')}
    `)}

    ${card('Registrar Schema de Teste', `
      <p class="text-xs text-gray-500 mb-3">Registra um JSON Schema chamado <code class="bg-gray-100 px-1 rounded">demo-order</code> com a estrutura de um pedido.</p>
      ${pBtn('📝 Registrar Schema','registerSchema()')}
      ${res('r-register')}
    `)}

    ${tip('"O Apicurio Registry armazena e versiona schemas que definem a estrutura dos dados trafegados no Kafka. Producers e consumers podem validar mensagens contra o schema registrado, garantindo que alterações incompatíveis sejam detectadas antes de causar falhas em produção."')}
  </div>`;
}

// ── Step 8: Data Contracts ───────────────────────────

function renderContracts() {
  return `<div class="space-y-5 fade-in">
    <div><h2 class="text-2xl font-bold">Data Contracts</h2><p class="text-gray-500 mt-1">Demonstração de compatibilidade, metadata, políticas de evolução e falha controlada.</p></div>

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
    <div><h2 class="text-2xl font-bold">Resumo da Demonstração</h2><p class="text-gray-500 mt-1">Tudo que foi demonstrado nesta POC.</p></div>

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
  } catch (e) { showErr('r-send', e.message); }
}

async function loadOrders() {
  showSpin('r-orders');
  try {
    const data = await api('/api/consumer/orders');
    const rows = (Array.isArray(data) ? data : []).map(o => [o.orderId || '-', o.customerName, o.product, o.quantity, o.price]);
    showTable('r-orders', ['ID', 'Cliente', 'Produto', 'Qtd', 'Preço'], rows);
    setHtml('r-count', `${rows.length} pedido(s)`);
  } catch (e) { showErr('r-orders', e.message); }
}

async function listTopics() {
  showSpin('r-topics');
  try {
    const data = await api('/api/kafka/topics');
    const html = data.map(t => `<button onclick="document.getElementById('t-name').value='${t}';describeTopic()" class="block w-full text-left px-3 py-2 text-sm rounded hover:bg-gray-100 font-mono">${esc(t)}</button>`).join('');
    setHtml('r-topics', `<div class="border rounded-lg divide-y max-h-64 overflow-y-auto mt-2">${html}</div>`);
  } catch (e) { showErr('r-topics', e.message); }
}

async function describeTopic() {
  const name = $('t-name').value.trim();
  if (!name) return;
  showSpin('r-topic-detail');
  try {
    const d = await api(`/api/kafka/topics/${encodeURIComponent(name)}`);
    const parts = (d.partitions || []).map(p => [p.partitionId, p.leader, (p.replicas||[]).map(r=>r.nodeId).join(','), (p.isr||[]).map(r=>r.nodeId).join(',')]);
    let h = `<p class="text-sm mb-2"><strong>Tópico:</strong> ${esc(name)} &nbsp; <strong>Partições:</strong> ${parts.length}</p>`;
    h += `<div class="overflow-x-auto rounded-lg border"><table class="min-w-full divide-y divide-gray-200"><thead class="bg-gray-50"><tr><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">Partição</th><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">Líder</th><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">Réplicas</th><th class="px-3 py-2 text-xs font-medium text-gray-500 text-left">ISR</th></tr></thead><tbody class="divide-y">${parts.map(r => `<tr>${r.map(c => `<td class="px-3 py-2 text-sm">${c}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
    setHtml('r-topic-detail', h);
  } catch (e) { showErr('r-topic-detail', e.message); }
}

// ── CDC helpers ──────────────────────────────────────

function cdcSql(base) {
  const upper = base === 'ora';
  const q = v => upper ? `'${v}'` : `'${v}'`;
  return { upper, q };
}

async function cdcInsert(base) {
  const rid = `r-${base}-insert`;
  showSpin(rid);
  try {
    const name = $(`${base}-name`).value, email = $(`${base}-email`).value, city = $(`${base}-city`).value;
    let sql;
    if (base === 'pg') {
      sql = `INSERT INTO customers (name, email, city) VALUES ('${name}', '${email}', '${city}')`;
    } else {
      sql = `INSERT INTO CUSTOMERS (NAME, EMAIL, CITY) VALUES ('${name}', '${email}', '${city}')`;
    }
    const data = await api(`/api/${base === 'pg' ? 'pg' : 'oracle'}/execute`, { method: 'POST', body: JSON.stringify({ sql }) });
    showOk(rid, { status: 'OK', ...data, sql });
  } catch (e) { showErr(rid, e.message); }
}

async function cdcUpdate(base) {
  const rid = `r-${base}-mod`;
  showSpin(rid);
  try {
    const id = $(`${base}-upd-id`).value, field = $(`${base}-upd-field`).value, val = $(`${base}-upd-val`).value;
    let sql;
    if (base === 'pg') {
      sql = `UPDATE customers SET ${field} = '${val}' WHERE id = ${id}`;
    } else {
      sql = `UPDATE CUSTOMERS SET ${field.toUpperCase()} = '${val}' WHERE ID = ${id}`;
    }
    const data = await api(`/api/${base === 'pg' ? 'pg' : 'oracle'}/execute`, { method: 'POST', body: JSON.stringify({ sql }) });
    showOk(rid, { status: 'OK', ...data, sql });
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
  } catch (e) { showErr(rid, e.message); }
}

async function cdcTable(base) {
  const rid = `r-${base}-table`;
  showSpin(rid);
  try {
    const data = await api(`/api/${base === 'pg' ? 'pg' : 'oracle'}/customers`);
    const headers = base === 'pg' ? ['id','name','email','city'] : ['ID','NAME','EMAIL','CITY'];
    const keys = base === 'pg' ? ['id','name','email','city'] : ['ID','NAME','EMAIL','CITY'];
    const rows = data.map(r => keys.map(k => r[k]));
    showTable(rid, headers, rows);
  } catch (e) { showErr(rid, e.message); }
}

async function cdcEvents(base, topic) {
  const rid = `r-${base}-cdc`;
  showSpin(rid);
  try {
    const msgs = await api(`/api/kafka/consume/${encodeURIComponent(topic)}?limit=5`);
    if (!msgs.length) return setHtml(rid, '<p class="text-gray-500 italic text-sm py-2">Nenhum evento encontrado (o consumer pode levar alguns segundos).</p>');
    const html = msgs.map(m => {
      let val = m.value;
      try { val = JSON.stringify(JSON.parse(m.value), null, 2); } catch (_) {}
      return `<div class="mb-3 border rounded-lg overflow-hidden">
        <div class="bg-gray-100 px-3 py-1.5 text-xs text-gray-600 flex gap-4"><span>Partição: ${m.partition}</span><span>Offset: ${m.offset}</span><span>Key: ${esc(m.key || 'null')}</span></div>
        <pre class="code text-xs" style="border-radius:0">${esc(val)}</pre>
      </div>`;
    }).join('');
    setHtml(rid, html);
  } catch (e) { showErr(rid, e.message); }
}

// ── Sink helpers ─────────────────────────────────────

async function sinkSource() {
  showSpin('r-sink-src');
  try {
    const data = await api('/api/oracle/customers');
    const rows = data.map(r => [r.ID, r.NAME, r.EMAIL, r.CITY]);
    showTable('r-sink-src', ['ID','Nome','Email','Cidade'], rows);
  } catch (e) { showErr('r-sink-src', e.message); }
}

async function sinkDest() {
  showSpin('r-sink-dst');
  try {
    const data = await api('/api/pg/oracle-customers');
    const keys = data.length ? Object.keys(data[0]) : [];
    const rows = data.map(r => keys.map(k => r[k]));
    showTable('r-sink-dst', keys, rows);
  } catch (e) { showErr('r-sink-dst', e.message); }
}

async function sinkStatus() {
  showSpin('r-sink-status');
  try {
    const data = await api('/api/kafka-connect/connectors/jdbc-sink-oracle-to-pg/status');
    showOk('r-sink-status', data);
  } catch (e) { showErr('r-sink-status', e.message); }
}

// ── Connect helpers ──────────────────────────────────

async function listConnectors() {
  showSpin('r-connectors');
  try {
    const data = await api('/api/kafka-connect/connectors');
    const names = Object.keys(data);
    if (!names.length) return setHtml('r-connectors', '<p class="text-gray-500 italic text-sm">Nenhum connector encontrado.</p>');
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
    setHtml('r-connectors', `<div class="space-y-2 mt-2">${html}</div>`);
  } catch (e) { showErr('r-connectors', e.message); }
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
  showSpin('r-artifacts');
  try {
    const data = await api('/api/apicurio/search/artifacts?limit=50');
    const artifacts = data.artifacts || [];
    if (!artifacts.length) return setHtml('r-artifacts', '<p class="text-gray-500 italic text-sm">Nenhum artifact registrado.</p>');
    const rows = artifacts.map(a => [a.artifactId, a.artifactType, a.name || '-', a.createdOn?.split('T')[0] || '-']);
    showTable('r-artifacts', ['Artifact ID', 'Tipo', 'Nome', 'Criado em'], rows);
  } catch (e) { showErr('r-artifacts', e.message); }
}

async function registerSchema() {
  showSpin('r-register');
  try {
    // Clean up existing artifact for repeatable demos
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
  step = Math.max(0, Math.min(STEPS.length - 1, i));
  $('step-content').innerHTML = RENDERS[step]();
  renderNav();
  $('step-indicator').textContent = `${step + 1} / ${STEPS.length}`;
  $('btn-prev').style.visibility = step === 0 ? 'hidden' : 'visible';
  $('btn-next').textContent = step === STEPS.length - 1 ? 'Concluir ✓' : 'Próximo →';
  $('step-content').parentElement.scrollTop = 0;
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

// ── Init ─────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  try { CFG = await api('/api/config'); } catch (_) { CFG = {}; }
  goTo(0);
});
