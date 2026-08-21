# Kafka Streaming POC — Red Hat Stack

Prova de conceito educacional de uma arquitetura de streaming baseada no stack Red Hat. Demonstra Kafka, Kafka Connect, CDC com Debezium (PostgreSQL e Oracle), Schema Registry com Apicurio e conceitos de Data Contracts — tudo implantado via GitOps com ArgoCD no OpenShift.

---

## Arquitetura

```text
                        +------------------------+
                        |   Apicurio Registry    |
                        |   (KafkaSQL backend)   |
                        +----------+-------------+
                                   |
                          schemas / validation
                                   |
              +--------------------+--------------------+
              |                                         |
        +-----+------+                          +------+------+
        |  Producer  |                          |  Consumer   |
        |  (Quarkus) |                          |  (Quarkus)  |
        +-----+------+                          +------+------+
              |                                        ^
              v                                        |
              +-------------> Kafka <------------------+
                          (3 brokers,
                           KRaft mode)
                              ^  ^  |
                              |  |  v
           +------------------+  +--+-----> JDBC Sink Connector
           |                     |         (Oracle CDC → PostgreSQL)
    +------+-------+      +-----+------+
    | Kafka Connect|      | Kafka Connect|
    | + Debezium   |      | + Debezium   |
    | (PostgreSQL) |      | (Oracle)     |
    +------+-------+      +------+-------+
           ^                      ^
           |                      |
    +------+-------+      +------+-------+
    | PostgreSQL   |      | Oracle Free  |
    | (12-el8)     |      | (23c-slim)   |
    +--------------+      +--------------+
```

**Componentes:**

| Componente | Versão | Função |
|---|---|---|
| Red Hat Streams for Apache Kafka | AMQ Streams 3.2.1 / Kafka 4.2.0 | Cluster Kafka em modo KRaft (3 brokers) |
| Red Hat build of Debezium | 3.6.1.Final | CDC para PostgreSQL e Oracle + JDBC Sink |
| Red Hat build of Apicurio Registry | 3.2.6 | Schema Registry com backend KafkaSQL |
| Quarkus | 3.38.3 (Java 21) | Aplicações Producer e Consumer |
| PostgreSQL | 12-el8 | Banco fonte para CDC + destino do Sink |
| Oracle Database Free | 23c-slim | Banco fonte para CDC via LogMiner |
| ArgoCD (OpenShift GitOps) | Latest | Deploy GitOps automático |

---

## Pré-requisitos

- Cluster **OpenShift 4.x** com acesso `cluster-admin`
- CLI `oc` instalado e autenticado no cluster
- Repositório público no GitHub (fork deste template)

---

## Quick Start

```bash
# 1. Clone o repositório
git clone https://github.com/dsferreira54/kafka-demo.git
cd kafka-demo

# 2. Autentique no OpenShift
oc login --server=https://api.<seu-cluster>:6443

# 3. Execute o deploy
bash deploy.sh

# 4. Acompanhe no ArgoCD (credenciais exibidas pelo script)
```

O script instala o ArgoCD, cria a Application e o cluster começa a sincronizar automaticamente. Todos os componentes são implantados em ~10 minutos.

> **Nota:** Após o deploy, o Oracle Database e o PostgreSQL requerem configuração manual de CDC (ARCHIVELOG, supplemental logging, replication slots). Consulte a seção 33 do `AGENTS.md` para detalhes.

---

## Validação

Após o deploy, verificar se o ambiente está saudável:

```bash
# ArgoCD Application
oc get application -n openshift-gitops kafka-demo \
  -o jsonpath='sync={.status.sync.status}  health={.status.health.status}'
# Esperado: sync=Synced  health=Healthy

# Pods
oc get pods -n kafka-demo --no-headers | grep -v build | awk '{print $1, $3}'
# Esperado: 11 pods Running

# Connectors
oc exec -n kafka-demo kafka-connect-connect-0 -- \
  curl -s http://localhost:8083/connectors
# Esperado: ["debezium-postgres","debezium-oracle","jdbc-sink-oracle-to-pg"]
```

---

## Credenciais e acessos

| Recurso | URL | Usuário / Senha |
|---|---|---|
| **Producer API** | https://producer.apps.cluster-gqpbh.dyn.redhatworkshops.io/api/orders | — (sem auth) |
| **Consumer API** | https://consumer.apps.cluster-gqpbh.dyn.redhatworkshops.io/api/orders | — (sem auth) |
| **Apicurio Registry UI** | https://apicurio-ui.apps.cluster-gqpbh.dyn.redhatworkshops.io | — (sem auth) |
| **Apicurio Registry API** | https://apicurio-api.apps.cluster-gqpbh.dyn.redhatworkshops.io/apis/registry/v3/ | — (sem auth) |
| **ArgoCD** | https://openshift-gitops-server-openshift-gitops.apps.cluster-gqpbh.dyn.redhatworkshops.io | `admin` / (ver output do `deploy.sh`) |
| **PostgreSQL** | Interno: `postgresql.kafka-demo.svc:5432` | `demouser` / `demo-password-123` / db: `demodb` |
| **Oracle Database** | Interno: `oracle.kafka-demo.svc:1521` | `C##DBZUSER` / `OracleDemo123` / SID: `FREE` / PDB: `FREEPDB1` |
| **Oracle app user** | (via PDB FREEPDB1) | `debezium` / `OracleDemo123` |

---

## Decisões técnicas

| Decisão | Motivo |
|---|---|
| Kafka KRaft (sem ZooKeeper) | AMQ Streams 3.2 suporta nativamente; reduz complexidade |
| Apicurio com KafkaSQL | Elimina dependência de banco externo para o Registry |
| Oracle Free 23c (não XE) | XE não suporta supplemental logging para CDC |
| JDBC Sink → PostgreSQL | Substituto educacional para GCS; mantém stack Red Hat |
| JSON Schema (não Avro) | Mais legível e didático para demonstração |
| Senhas diretas nos CRs | Workaround para bug Debezium 3.6.1 + Kafka Connect 4.2.0 |

Para detalhes completos, consulte a seção 33 do `AGENTS.md`.

---

## Limitações conhecidas

- **Debezium 3.6.1 + Kafka Connect 4.2.0:** Bug de validação (`NullPointerException`) no endpoint PUT. Conectores funcionam normalmente, mas KafkaConnector CRs podem aparecer momentaneamente como "Degraded" durante reconciliação.
- **Oracle ARCHIVELOG:** Precisa ser habilitado manualmente após cada restart do pod Oracle (dados em `emptyDir`).
- **PostgreSQL replication:** O role `REPLICATION` precisa ser concedido manualmente ao `demouser` após recriação do pod.
- **Senhas em CRs:** As senhas estão nos KafkaConnector CRs como plaintext (aceitável para POC, não para produção).
- **Sem TLS interno:** Comunicação entre componentes usa plaintext (adequado para lab).
- **Sem autenticação nas apps:** Producer e Consumer não exigem autenticação.

---

## Roteiro de demonstração

Este roteiro guia uma apresentação completa da POC, do mais básico ao mais avançado. Tempo estimado: **20–30 minutos**.

### Passo 1 — Visão geral no ArgoCD

**Abrir:** [ArgoCD UI](https://openshift-gitops-server-openshift-gitops.apps.cluster-gqpbh.dyn.redhatworkshops.io)

**Mostrar:**
- Application `kafka-demo` com status **Synced** e **Healthy**
- Árvore de recursos: namespaces, operadores, Kafka cluster, databases, apps, connectors
- Clicar em alguns recursos para mostrar a hierarquia

**Explicar:**
> "Todo o ambiente é declarado como código no Git. O ArgoCD monitora o repositório e aplica automaticamente qualquer mudança. Se alguém alterar algo manualmente no cluster, o ArgoCD reverte. Isso é GitOps."

---

### Passo 2 — Kafka: produzir e consumir um evento

**Abrir:** Terminal

**Mostrar:** Enviar um pedido via Producer e ler via Consumer.

```bash
# Enviar um pedido
curl -sk -X POST https://producer.apps.cluster-gqpbh.dyn.redhatworkshops.io/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerName":"Maria Silva","product":"Red Hat OpenShift","quantity":1,"price":15000}'
```

```json
{"orderId":"a1b2c3d4","customerName":"Maria Silva","product":"Red Hat OpenShift","quantity":1,"price":15000.0,"timestamp":"2026-08-21T..."}
```

```bash
# Consultar pedidos consumidos
curl -sk https://consumer.apps.cluster-gqpbh.dyn.redhatworkshops.io/api/orders | python3 -m json.tool
```

**Explicar:**
> "O Producer é uma aplicação Quarkus que recebe pedidos via REST e publica no Kafka. O Consumer é outra aplicação que consome do mesmo tópico. Isso demonstra o padrão básico de mensageria: um publisher, um broker, um subscriber."

---

### Passo 3 — Explorar tópicos e partições

**Abrir:** Terminal

**Mostrar:**

```bash
# Listar tópicos
oc exec -n kafka-demo kafka-cluster-combined-0 -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

```bash
# Detalhar o tópico orders
oc exec -n kafka-demo kafka-cluster-combined-0 -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```

**Explicar:**
> "O Kafka organiza mensagens em tópicos. Cada tópico tem partições (neste caso, 3) e cada partição é replicada em 3 brokers. As mensagens são distribuídas entre as partições usando a chave como critério — mensagens com a mesma chave sempre vão para a mesma partição, garantindo ordenação por chave."

---

### Passo 4 — CDC PostgreSQL: capturando mudanças no banco

**Abrir:** Terminal (duas janelas lado a lado — uma para SQL, outra para Kafka)

**Mostrar:**

```bash
# Terminal 1: Inserir no PostgreSQL
PG_POD=$(oc get pods -n kafka-demo -l app=postgresql -o jsonpath='{.items[0].metadata.name}')

oc exec -n kafka-demo $PG_POD -- \
  psql -U demouser -d demodb -c \
  "INSERT INTO customers (name, email, city) VALUES ('João Demo', 'joao@demo.com', 'São Paulo');"
```

Aguardar ~5 segundos e ler os eventos CDC:

```bash
# Terminal 2: Ler eventos CDC do Kafka
oc exec -n kafka-demo kafka-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic debezium.public.customers \
  --from-beginning --max-messages 5 --timeout-ms 10000
```

Repetir com UPDATE e DELETE:

```bash
# UPDATE
oc exec -n kafka-demo $PG_POD -- \
  psql -U demouser -d demodb -c \
  "UPDATE customers SET city = 'Rio de Janeiro' WHERE name = 'João Demo';"

# DELETE
oc exec -n kafka-demo $PG_POD -- \
  psql -U demouser -d demodb -c \
  "DELETE FROM customers WHERE name = 'João Demo';"
```

**Explicar:**
> "O Debezium monitora o write-ahead log (WAL) do PostgreSQL em tempo real. Qualquer INSERT, UPDATE ou DELETE gera automaticamente um evento no Kafka. Isso é Change Data Capture — o banco de dados vira uma fonte de eventos sem precisar alterar a aplicação."

---

### Passo 5 — CDC Oracle: mesmo conceito, diferente banco

**Abrir:** Terminal

**Mostrar:**

```bash
# Inserir no Oracle
ORACLE_POD=$(oc get pods -n kafka-demo -l app=oracle -o jsonpath='{.items[0].metadata.name}')

oc exec -n kafka-demo $ORACLE_POD -- bash -c "
sqlplus -S debezium/OracleDemo123@//localhost:1521/FREEPDB1 <<EOF
INSERT INTO CUSTOMERS (NAME, EMAIL, CITY) VALUES ('Ana Demo', 'ana@demo.com', 'Brasilia');
COMMIT;
EOF
"
```

Aguardar ~10 segundos e ler os eventos:

```bash
# Ler eventos CDC do Oracle
oc exec -n kafka-demo kafka-cluster-combined-0 -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic oracle.DEBEZIUM.CUSTOMERS \
  --from-beginning --max-messages 5 --timeout-ms 15000
```

**Explicar:**
> "O mesmo Debezium que captura mudanças do PostgreSQL também funciona com Oracle — usando LogMiner em vez do WAL. A aplicação não precisa saber de nada: o Debezium cuida de toda a integração. O Kafka Connect gerencia ambos os conectores."

---

### Passo 6 — Sink Connector: dados fluindo para destino externo

**Abrir:** Terminal

**Mostrar:**

```bash
# Verificar status do Sink Connector
oc exec -n kafka-demo kafka-connect-connect-0 -- \
  curl -s http://localhost:8083/connectors/jdbc-sink-oracle-to-pg/status | python3 -m json.tool
```

```bash
# Consultar dados replicados no PostgreSQL (vindos do Oracle via Kafka)
PG_POD=$(oc get pods -n kafka-demo -l app=postgresql -o jsonpath='{.items[0].metadata.name}')

oc exec -n kafka-demo $PG_POD -- \
  psql -U demouser -d demodb -c "SELECT id, name, city FROM oracle_customers ORDER BY id;"
```

**Explicar:**
> "O JDBC Sink Connector consome eventos do tópico CDC do Oracle e escreve automaticamente no PostgreSQL. Isso demonstra o padrão Sink: dados saem do Kafka para um destino externo. Em produção, o destino poderia ser S3, GCS, Elasticsearch ou qualquer sistema suportado. O conceito é o mesmo."

---

### Passo 7 — Kafka Connect: visão operacional

**Abrir:** Terminal

**Mostrar:**

```bash
# Listar todos os connectors
oc exec -n kafka-demo kafka-connect-connect-0 -- \
  curl -s http://localhost:8083/connectors | python3 -m json.tool

# Status detalhado de cada um
for c in debezium-postgres debezium-oracle jdbc-sink-oracle-to-pg; do
  echo "=== $c ==="
  oc exec -n kafka-demo kafka-connect-connect-0 -- \
    curl -s "http://localhost:8083/connectors/$c/status" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  type={d[\"type\"]} state={d[\"connector\"][\"state\"]} tasks={len(d[\"tasks\"])}')"
done
```

**Explicar:**
> "O Kafka Connect é um framework para integração. Ele gerencia conectores (source e sink) como unidades operacionais com estado, configuração e ciclo de vida próprios. Cada conector pode ter múltiplas tasks para paralelismo. A API REST permite monitorar e gerenciar tudo programaticamente."

---

### Passo 8 — Schema Registry: registrar e versionar schemas

**Abrir:** [Apicurio Registry UI](https://apicurio-ui.apps.cluster-gqpbh.dyn.redhatworkshops.io)

**Mostrar:**
- Lista de artifacts registrados (3 schemas)
- Clicar em `order-event-contract` → mostrar versões (v1.0.0, v1.1.0)
- Mostrar o conteúdo do schema (JSON Schema com campos, tipos, restrições)
- Mostrar as rules (COMPATIBILITY: BACKWARD)

Alternativamente, via API:

```bash
# Listar artifacts
curl -sk https://apicurio-api.apps.cluster-gqpbh.dyn.redhatworkshops.io/apis/registry/v3/search/artifacts | \
  python3 -c "import sys,json; [print(f'  {a[\"artifactId\"]} ({a[\"artifactType\"]})') for a in json.load(sys.stdin)['artifacts']]"

# Ver versões
curl -sk https://apicurio-api.apps.cluster-gqpbh.dyn.redhatworkshops.io/apis/registry/v3/groups/default/artifacts/order-event-contract/versions | \
  python3 -c "import sys,json; [print(f'  v{v[\"version\"]} state={v[\"state\"]}') for v in json.load(sys.stdin)['versions']]"
```

**Explicar:**
> "O Apicurio Registry armazena e versiona schemas. Cada tópico pode ter um schema associado que define a estrutura dos eventos. Isso garante que producers e consumers concordem sobre o formato dos dados."

---

### Passo 9 — Data Contracts: compatibilidade e falha controlada

**Abrir:** Terminal

**Mostrar:** Tentar registrar uma versão **incompatível** (adicionar campo obrigatório):

```bash
# Tentativa de evolução incompatível: adicionar campo obrigatório "priority"
curl -sk -X POST \
  https://apicurio-api.apps.cluster-gqpbh.dyn.redhatworkshops.io/apis/registry/v3/groups/default/artifacts/order-event-contract/versions \
  -H "Content-Type: application/json" \
  -d '{
    "version": "99.0.0",
    "content": {
      "content": "{\"type\":\"object\",\"required\":[\"orderId\",\"customerName\",\"product\",\"quantity\",\"price\",\"timestamp\",\"priority\"],\"properties\":{\"orderId\":{\"type\":\"string\"},\"customerName\":{\"type\":\"string\"},\"product\":{\"type\":\"string\"},\"quantity\":{\"type\":\"integer\"},\"price\":{\"type\":\"number\"},\"timestamp\":{\"type\":\"string\"},\"priority\":{\"type\":\"string\"}}}",
      "contentType": "application/json"
    }
  }' | python3 -m json.tool
```

Resultado esperado — **HTTP 409** com explicação:

```json
{
    "title": "Incompatible artifact: order-event-contract [JSON], num of incompatible diffs: {1}, ...",
    "status": 409,
    "causes": [
        {
            "description": "OBJECT_TYPE_REQUIRED_PROPERTIES_MEMBER_ADDED",
            "context": "/required"
        }
    ]
}
```

Agora mostrar os **labels** do Data Contract:

```bash
# Metadata do contrato
curl -sk https://apicurio-api.apps.cluster-gqpbh.dyn.redhatworkshops.io/apis/registry/v3/groups/default/artifacts/order-event-contract | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f'Nome: {d[\"name\"]}')
print(f'Descrição: {d[\"description\"]}')
print('Labels:')
for k, v in sorted(d.get('labels', {}).items()):
    print(f'  {k}: {v}')
"
```

**Explicar:**
> "Data Contracts vão além do schema: incluem ownership (quem é responsável), classificação dos dados, SLA, política de compatibilidade e quais aplicações produzem e consomem. O Apicurio Registry implementa nativamente schema, versionamento, compatibilidade, labels e validação. O enforcement no broker (rejeitar mensagens inválidas) NÃO é nativo — depende de serializers nas aplicações ou mecanismos externos."

---

### Passo 10 — Resumo e encerramento

**Abrir:** Diagrama da arquitetura (seção Arquitetura deste README ou slide)

**Explicar:**

> "Nesta demonstração, vimos na prática:
>
> 1. **Kafka** como plataforma central de streaming (3 brokers, KRaft, tópicos, partições)
> 2. **Producer e Consumer** trocando eventos via Kafka (Quarkus, SmallRye Reactive Messaging)
> 3. **CDC com Debezium** capturando mudanças de PostgreSQL (via WAL/pgoutput) e Oracle (via LogMiner)
> 4. **Kafka Connect** gerenciando integrações como Source e Sink Connectors
> 5. **Sink Connector** replicando dados do Kafka para um destino externo (JDBC → PostgreSQL)
> 6. **Apicurio Registry** registrando, versionando e validando schemas
> 7. **Data Contracts** com metadata, labels, políticas de compatibilidade e validação
> 8. **GitOps** com ArgoCD: todo o estado declarado em Git, sync automático
>
> Todos estes componentes são do stack Red Hat e podem ser utilizados em substituição a plataformas como Confluent."
