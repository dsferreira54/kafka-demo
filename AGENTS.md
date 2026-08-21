# AGENTS.md

## 1. Sobre este template

Este repositório é um **template de modo de trabalho** para demos em Red Hat OpenShift utilizando GitOps (ArgoCD + Helm).

O fluxo de uso é:

1. Criar um **fork** deste template para cada nova demo.
2. Clonar o fork.
3. Preencher este `AGENTS.md` com o contexto e objetivos da demo.
4. Executar `deploy.sh` no cluster OpenShift (instala ArgoCD e aponta para o repositório).
5. Desenvolver a demo adicionando manifestos no Helm chart (`gitops/templates/`).
6. O ArgoCD sincroniza automaticamente as alterações no cluster.

---

## 2. Objetivo da demo

Este repositório existe para construir uma **prova de conceito educacional** de uma arquitetura de streaming baseada no stack Red Hat.

A demo **não** é um deliverable para cliente e não deve tentar reproduzir fielmente um ambiente de produção. O objetivo principal é permitir que o responsável pela oportunidade entenda, na prática, como os principais componentes se conectam, quais responsabilidades cada um possui e quais pontos exigiriam atenção em uma migração real.

O cenário de negócio que motivou o estudo envolve uma possível substituição de uma plataforma Confluent por componentes Red Hat. Entretanto, **esta demo deve implementar somente o lado Red Hat**. Não implementar Confluent nesta fase.

### 2.1 Contexto da oportunidade

O cenário original possui Apache Kafka como plataforma central de streaming e integrações ao redor dele, incluindo:

- Kafka
- Kafka Connect
- CDC de Oracle
- CDC de PostgreSQL
- integração de saída para armazenamento
- Schema Registry
- conceitos de Data Contracts
- aplicações produtoras e consumidoras

O ambiente real é significativamente maior, com muitos tópicos, grande volume de dados, vários conectores e requisitos de rede e segurança.

Esta demo deve deliberadamente reduzir essa complexidade ao mínimo necessário para demonstrar os conceitos.

### 2.2 Resultado esperado

Ao terminar a demo, alguém com conhecimento limitado de Kafka deve conseguir olhar para o ambiente e entender, na prática:

```text
Quem publica eventos?           →  Producer
Onde eles ficam?                 →  Kafka
Quem os lê?                     →  Consumer
Como bancos viram eventos?       →  Debezium + Kafka Connect
Como Kafka integra com destinos? →  Sink Connector
Onde ficam os schemas?           →  Apicurio Registry
Como schemas evoluem?            →  Compatibility / validation
Onde entram Data Contracts?      →  Schema + metadata + regras/políticas
                                    aplicadas pelos mecanismos realmente
                                    disponíveis no stack escolhido
```

Esse entendimento prático é o principal critério de valor desta demo.

---

## 3. Fases do projeto

### Fase 1 — Estrutura básica e GitOps

- Preencher este `AGENTS.md` com o contexto da demo.
- Executar `deploy.sh` para instalar o ArgoCD e configurar a Application.
- Validar que o ArgoCD sincroniza com o repositório.

### Fase 2 — Red Hat Streams for Apache Kafka

- Instalar o operador Red Hat Streams for Apache Kafka.
- Criar um cluster Kafka funcional.
- Criar tópicos e validar produção/consumo básico via CLI.

### Fase 3 — Apicurio Registry

- Instalar o operador Red Hat build of Apicurio Registry.
- Criar uma instância do Registry funcional.
- Validar acesso e registro de schemas.

### Fase 4 — Aplicações Producer e Consumer

- Criar aplicação Producer (linguagem a definir durante implementação).
- Criar aplicação Consumer.
- Integrar com Kafka e com o Apicurio Registry.
- Validar fluxo completo: produção → Kafka → consumo.

### Fase 5 — PostgreSQL CDC com Debezium

- Subir instância PostgreSQL.
- Configurar Kafka Connect com Debezium para PostgreSQL.
- Validar CDC: INSERT, UPDATE, DELETE → eventos no Kafka.

### Fase 6 — Oracle CDC com Debezium

- Subir instância Oracle Database adequada para laboratório.
- Configurar Kafka Connect com Debezium para Oracle.
- Validar CDC: INSERT, UPDATE, DELETE → eventos no Kafka.

### Fase 7 — Sink Connector

- Escolher destino local/facilmente implantável para substituir educacionalmente o GCS.
- Configurar Sink Connector.
- Validar fluxo: Kafka → Sink Connector → destino externo.

### Fase 8 — Schema Evolution e Data Contracts

- Demonstrar versionamento e compatibilidade de schemas no Apicurio.
- Testar evolução compatível e incompatível.
- Explorar conceitos de Data Contracts com as capacidades reais do stack.
- Documentar o que é nativo do Apicurio, o que depende da aplicação e o que não possui representação direta.

### Fase 9 — Experimentos e documentação final

- Executar os experimentos mínimos (A a G).
- Documentar versões, decisões, alternativas e limitações.
- Finalizar README com instruções de reprodução.

---

## 4. Estrutura autoritativa do repositório

```text
kafka-demo/
├── AGENTS.md              # Regras, contexto e orientações para agentes
├── deploy.sh              # Bootstrap: instala ArgoCD + cria Application
├── README.md              # Documentação de uso e validação
├── .gitignore
├── apps/                  # Código-fonte de aplicações da demo
│   └── .gitkeep
└── gitops/
    ├── Chart.yaml         # Metadados do Helm chart
    ├── .helmignore
    ├── values.yaml        # Valores configuráveis (namespaces, flags, recursos)
    └── templates/         # Manifestos Kubernetes (gerenciados pelo ArgoCD)
        └── .gitkeep
```

Responsabilidades:

- `gitops/` — Helm chart e manifestos declarativos. Fonte de verdade para o ArgoCD.
- `gitops/templates/` — Todos os manifestos Kubernetes que o ArgoCD aplica no cluster. Organizar em subdiretórios numerados por componente (ex.: `1-operador/`, `2-aplicacao/`).
- `apps/` — Código-fonte de aplicações auxiliares da demo (Dockerfiles, código da aplicação, assets estáticos, etc.). Estas aplicações são compiladas no cluster via **BuildConfig** (ver seção 7).
- `deploy.sh` — Bootstrap do ArgoCD. Detecta automaticamente a URL do repositório via `git remote`.
- `README.md` — Documentação do projeto.
- `AGENTS.md` — Regras e orientações para agentes de IA.

---

## 5. Ambiente de trabalho

### Cluster OpenShift

- Cluster de **laboratório** — pode ser quebrado sem penalização.
- Acesso como `admin` via `oc` CLI.
- Se o cluster ficar inacessível, informar o operador para que suba um novo.

### Repositório GitHub

- Repositório **público** (sem necessidade de autenticação no ArgoCD).
- Branch principal: `main`.
- Credenciais do GitHub configuradas via credential helper (push automático).
- Commits e pushes são permitidos a qualquer momento.

### ArgoCD

- Instalado pelo `deploy.sh`.
- A Application aponta para o diretório `gitops/` deste repositório.
- Sync automático com `prune: true`, `selfHeal: true`.
- `SkipDryRunOnMissingResource=true` e `CreateNamespace=true` habilitados.
- Retry automático com backoff exponencial (10s → 3m, ilimitado).
- Intervalo de sync rápido: 5 segundos.
- Controller com `cluster-admin` para gerenciar qualquer recurso.

---

## 6. Regras de trabalho com GitOps

### Regra de ouro

> **Toda alteração no cluster deve estar refletida no repositório Git.**

O ArgoCD é a fonte de verdade. Alterações manuais via `oc` são permitidas temporariamente para diagnóstico ou prototipação, mas **devem ser transcritas para o Helm chart antes de considerar o trabalho concluído**.

### Fluxo permitido

1. Prototipar com `oc apply` diretamente no cluster (se necessário).
2. Validar que funciona.
3. Transcrever para template Helm em `gitops/templates/`.
4. Commit e push.
5. Verificar sincronização no ArgoCD.

### ArgoCD pode ser pausado

- É permitido pausar o sync automático durante testes manuais.
- Reativar o sync após transcrever as alterações para o repositório.

### Quando recriar a Application

Se o ArgoCD ficar com estado inconsistente (sync travado, recursos órfãos), é seguro deletar e recriar a Application:

```bash
oc delete application <nome> -n openshift-gitops
bash deploy.sh
```

Isso não afeta os recursos já aplicados no cluster — apenas recria o controle do ArgoCD.

---

## 7. Deploy de aplicações com BuildConfig

Quando a demo inclui aplicações próprias (em `apps/`), o padrão recomendado para deploy no OpenShift é usar **BuildConfig + ImageStream**. Isso garante reprodutibilidade e elimina dependência de registries externos.

### Padrão

Para cada aplicação em `apps/`, criar no Helm chart (`gitops/templates/`) os seguintes recursos:

1. **ImageStream** — registro da imagem no registry interno do OpenShift.
2. **BuildConfig** — compila o código-fonte diretamente do repositório Git e faz push para o ImageStream. Usar estratégia `Source` (S2I) com builder images do OpenShift (ex.: `ubi8-openjdk-21:1.18`, `httpd:2.4-ubi9`, `nodejs:20-ubi9`).
3. **Deployment** — referencia a imagem do registry interno: `image-registry.openshift-image-registry.svc:5000/<namespace>/<nome>:latest`.
4. **Service** — expõe o Deployment internamente.
5. **Route** — expõe o Service externamente com TLS edge.

### Exemplo de referência no Deployment

```yaml
image: image-registry.openshift-image-registry.svc:5000/{{ .Values.namespaces.apps }}/minha-app:latest
imagePullPolicy: Always
```

### Exemplo de BuildConfig

```yaml
apiVersion: build.openshift.io/v1
kind: BuildConfig
metadata:
  name: minha-app
  namespace: {{ .Values.namespaces.apps }}
spec:
  source:
    type: Git
    git:
      uri: {{ .Values.minhaApp.git.uri | quote }}
      ref: {{ .Values.minhaApp.git.ref | quote }}
    contextDir: {{ .Values.minhaApp.git.contextDir | quote }}
  strategy:
    type: Source
    sourceStrategy:
      from:
        kind: ImageStreamTag
        namespace: openshift
        name: "ubi8-openjdk-21:1.18"
  output:
    to:
      kind: ImageStreamTag
      name: minha-app:latest
  triggers:
    - type: ConfigChange
```

### Valores correspondentes no `values.yaml`

```yaml
minhaApp:
  enabled: true
  git:
    uri: https://github.com/<usuario>/<repositorio>
    ref: main
    contextDir: apps/minha-app
```

Dessa forma, o ArgoCD cria o BuildConfig, o OpenShift compila o código-fonte do próprio repositório, faz push para o registry interno, e o Deployment usa essa imagem automaticamente.

---

## 8. Organização dos templates Helm

### Subdiretórios numerados

Organizar `gitops/templates/` em subdiretórios numerados por componente ou camada:

```text
gitops/templates/
├── 1-operador/
│   ├── 0-operator.yaml
│   ├── 1-custom-resource.yaml
│   └── ...
├── 2-aplicacao/
│   ├── 0-namespace.yaml
│   ├── 1-deployment.yaml
│   └── ...
└── 3-integracao/
    └── ...
```

### Sync-waves

Usar anotações `argocd.argoproj.io/sync-wave` para controlar a ordem de aplicação:

- `"10"` — Operadores (Subscriptions OLM)
- `"20"` — Custom Resources que dependem dos CRDs do operador
- `"30"` a `"50"` — Recursos de infraestrutura (namespaces, configs)
- `"60"` a `"80"` — Aplicações (BuildConfig, Deployment, Service, Route)

Waves mais altos são aplicados depois. Recursos sem anotação são aplicados na wave `"0"`.

### Labels padrão

Usar labels padronizadas em todos os recursos:

```yaml
labels:
  app.kubernetes.io/name: <nome-do-recurso>
  app.kubernetes.io/part-of: kafka-demo
  app.kubernetes.io/managed-by: Helm
```

### Health checks

Sempre configurar `readinessProbe` e `livenessProbe` em Deployments:

```yaml
readinessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 10
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 20
  periodSeconds: 15
```

### Limites de recursos

Sempre definir `resources.requests` e `resources.limits` em Deployments. Parametrizar via `values.yaml`:

```yaml
resources:
  requests:
    cpu: {{ .Values.app.resources.requests.cpu | quote }}
    memory: {{ .Values.app.resources.requests.memory | quote }}
  limits:
    cpu: {{ .Values.app.resources.limits.cpu | quote }}
    memory: {{ .Values.app.resources.limits.memory | quote }}
```

---

## 9. Instalação de operadores via GitOps

Ao instalar operadores OLM cujos CRDs ainda não existem no cluster:

### Padrão

1. Criar o `Subscription` no Helm chart com sync-wave baixo (ex.: `"10"`).
2. Criar os CRs (Custom Resources) com sync-wave mais alto (ex.: `"20"`).
3. O `deploy.sh` já configura `SkipDryRunOnMissingResource=true` na Application — o ArgoCD não falha ao tentar validar CRDs inexistentes.
4. O retry com backoff garante que recursos que falharam na primeira tentativa sejam reaplicados automaticamente quando os CRDs estiverem disponíveis.

### Fallback manual

Se o ArgoCD não conseguir avançar (estado "Running" com recursos "OutOfSync Missing"):

1. Aplicar o `Subscription` manualmente: `oc apply -f gitops/templates/1-operador/0-operator.yaml`.
2. Aguardar o operador instalar e criar seus CRDs.
3. O ArgoCD assume o controle na próxima sincronização e aplica os CRs restantes.

### Exemplo de Subscription

```yaml
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: meu-operador
  namespace: openshift-operators
  annotations:
    argocd.argoproj.io/sync-wave: "10"
spec:
  channel: stable
  installPlanApproval: Automatic
  name: meu-operador
  source: redhat-operators
  sourceNamespace: openshift-marketplace
```

---

## 10. Parametrização com `values.yaml`

Usar `values.yaml` para centralizar toda configuração variável. Padrões recomendados:

```yaml
# Domínio de ingress (preenchido automaticamente pelo deploy.sh)
ingressDomain: ""

# Namespaces
namespaces:
  apps: kafka-demo

# Feature flags para habilitar/desabilitar componentes
operador:
  enabled: true
  channel: stable

# Configuração de aplicações
minhaApp:
  enabled: true
  replicas: 1
  routeHost: minha-app
  git:
    uri: https://github.com/<usuario>/<repositorio>
    ref: main
    contextDir: apps/minha-app
  resources:
    requests:
      cpu: 200m
      memory: 256Mi
    limits:
      cpu: "1"
      memory: 512Mi
```

Usar condicionais `{{- if .Values.xxx.enabled }}` nos templates para habilitar/desabilitar blocos inteiros.

---

## 11. Exposição de serviços

### Route (padrão OpenShift)

Para expor um Service via Route OpenShift com TLS:

```yaml
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: minha-app
  namespace: {{ .Values.namespaces.apps }}
spec:
  host: {{ printf "%s.%s" .Values.minhaApp.routeHost .Values.ingressDomain | quote }}
  to:
    kind: Service
    name: minha-app
  port:
    targetPort: http
  tls:
    termination: edge
    insecureEdgeTerminationPolicy: Redirect
```

O `ingressDomain` é detectado automaticamente pelo `deploy.sh` e passado como parâmetro Helm para o ArgoCD.

### Gateway API

Se a demo utilizar Gateway API (ex.: com Connectivity Link / Kuadrant / Istio), criar Gateway + HTTPRoute separadamente dos Routes padrão. Em bare-metal, pode ser necessário criar um Route apontando para o Service do Gateway manualmente.

---

## 12. Política de commits

### Regras

- Fazer commits incrementais e lógicos — nem muito grandes, nem triviais.
- Cada commit deve representar uma unidade coerente de trabalho.
- Mensagens de commit devem ser claras e descritivas.
- Não acumular muitas alterações em um único commit.
- Não fazer commit para alterar uma única linha, a menos que seja realmente necessário.

### Formato

```text
feat: add <componente> subscription and custom resources
feat: add <app> deployment with BuildConfig
fix: correct <descrição concisa do problema>
docs: update README with <seção>
refactor: simplify <componente> template
```

---

## 13. Documentação

O `README.md` é o ponto de entrada principal do repositório. Ele deve permitir que qualquer pessoa — mesmo sem contexto prévio — entenda o que a demo faz, como subir o ambiente e como apresentá-la.

### 13.1 Seções obrigatórias do README.md

O `README.md` deve conter **obrigatoriamente** as seguintes seções, nesta ordem:

1. **Título e descrição** — Nome do projeto e resumo de 2-3 frases explicando o que é, para que serve e qual stack utiliza.
2. **Arquitetura** — Diagrama ASCII ou Mermaid mostrando os componentes e suas conexões. Deve ser compreensível sem ler o resto do documento.
3. **Pré-requisitos** — O que é necessário antes de executar (cluster, CLIs, acessos, credenciais).
4. **Quick Start** — Passos mínimos para subir o ambiente do zero. No máximo 5 comandos.
5. **Validação** — Como verificar que o ambiente está saudável após o deploy. Incluir comandos `oc` e `curl` com output esperado.
6. **Credenciais e acessos** — Tabela com todas as URLs, usuários e senhas fictícias. Incluir links das Routes, ArgoCD, consoles e APIs.
7. **Decisões técnicas** — Resumo das escolhas feitas durante a implementação e por quê. Referenciar a seção 33 do AGENTS.md para detalhes completos.
8. **Limitações conhecidas** — O que esta demo não faz, o que é diferente de produção e quais workarounds existem.
9. **Roteiro de demonstração** — Guia completo e cronológico para apresentar a demo (ver seção 13.2).

### 13.2 Roteiro de demonstração

O roteiro de demonstração é a seção mais importante do README para quem vai **apresentar** a demo para outra pessoa. Ele deve ser um guia passo a passo cronológico que qualquer pessoa possa seguir.

Para **cada passo** do roteiro, documentar:

- **O que abrir** — qual URL, console, terminal ou arquivo/YAML apresentar na tela.
- **O que mostrar** — o que a audiência deve ver na tela (output esperado, dados, interface).
- **O que explicar** — qual o valor, conceito ou insight sendo demonstrado naquele passo.

O roteiro deve:

- Seguir uma **ordem lógica** do mais básico ao mais avançado.
- Incluir **links** e **comandos exatos** prontos para copiar e colar.
- Cobrir **cenários de sucesso** e, quando aplicável, **cenários de falha controlada** (ex.: tentativa de registrar schema incompatível).
- Finalizar com um **resumo** do que foi demonstrado.

Estrutura sugerida do roteiro:

1. Visão geral no ArgoCD (todos os componentes sincronizados).
2. Kafka básico (produzir e consumir um evento).
3. Explorar tópicos e partições.
4. CDC PostgreSQL (INSERT, UPDATE, DELETE → eventos no Kafka).
5. CDC Oracle (mesmo fluxo, diferente banco).
6. Sink Connector (dados fluindo do Kafka para destino externo).
7. Schema Registry (registrar, versionar, testar compatibilidade).
8. Data Contracts (metadata, labels, políticas, falha controlada).
9. Resumo e encerramento.

### 13.3 Diretrizes gerais

- Linguagem clara e acessível, em português.
- Usar code blocks para todos os comandos e outputs.
- Incluir `curl` com `-sk` para HTTPS com certificados auto-assinados.
- Usar URLs reais (com domínio do cluster) nos exemplos quando disponíveis.
- Preferir exemplos que possam ser executados imediatamente, sem adaptação.
- Documentar credenciais fictícias explicitamente — nunca assumir que o leitor sabe.
- Manter o README atualizado sempre que o estado da demo mudar.

---

## 14. Princípios de conteúdo

Todo conteúdo deve ser genérico e reaproveitável.

Não citar:

- Clientes reais.
- Instituições reais.
- Informações comerciais sensíveis.
- Dados pessoais reais.

---

## 15. Segurança

- Usar apenas dados fictícios.
- Não comitar segredos reais (tokens, senhas, credenciais).
- Proteger segredos com Kubernetes Secrets criados fora do repositório quando necessário.
- Não expor tokens ou credenciais em código ou documentação.
- Credenciais de demo devem ser fictícias e estar documentadas no `README.md`.
- Utilizar mecanismos apropriados para secrets no ambiente escolhido.
- Não é necessário reproduzir o modelo corporativo completo de API Keys, Service Accounts, ACLs, certificados ou integração com identidade corporativa. Demonstrar de forma mínima se forem necessários para o funcionamento da plataforma.

---

## 16. Forma de trabalho do agente

Antes de implementar:

1. Ler este `AGENTS.md`.
2. Examinar o estado atual dos arquivos e do cluster.
3. Relacionar a mudança a uma fase ou requisito do projeto.
4. Escolher a solução mais simples.
5. Evitar mudanças não solicitadas.

Durante:

1. Fazer mudanças incrementais.
2. Validar cada etapa (no cluster e/ou com `helm template`).
3. Não esconder falhas.
4. Registrar decisões relevantes.
5. Não introduzir dependências sem justificativa.
6. Não alterar a estrutura do repositório sem autorização.

Depois:

1. Executar testes e validações aplicáveis.
2. Revisar o diff antes de comitar.
3. Verificar que não há segredos expostos.
4. Confirmar estado do ArgoCD.
5. Informar limitações e riscos.

---

## 17. Formato de resposta do agente

Ao concluir uma atividade, reportar:

- **Objetivo**: Problema resolvido.
- **Alterações**: Arquivos criados/modificados.
- **Validação**: Comandos e testes executados.
- **Resultado**: O que está funcionando.
- **Limitações**: O que não foi coberto.
- **Riscos**: Pontos de atenção.
- **Próxima ação recomendada**: Próximo passo lógico.

Regra: não afirmar que funciona sem validação.

---

## 18. Restrições

Não fazer:

- Reorganizar a estrutura do repositório sem solicitação explícita.
- Implementar complexidade fora do escopo da fase atual.
- Expor segredos, tokens ou credenciais.
- Usar dados reais.
- Tratar a demonstração como arquitetura final de produção.

---

## 19. Critérios de sucesso

A demo estará concluída quando:

1. Red Hat Streams for Apache Kafka estiver funcional.
2. Producer e consumer trocarem eventos com sucesso.
3. Kafka Connect estiver sendo utilizado.
4. PostgreSQL CDC estiver funcionando via Red Hat build of Debezium.
5. Oracle CDC estiver funcionando via Red Hat build of Debezium.
6. Apicurio Registry estiver operacional e integrado a pelo menos um fluxo.
7. Versionamento e compatibilidade de schema tiverem sido demonstrados.
8. Conceitos de Data Contract tiverem sido testados sem atribuir ao produto capacidades que ele não possui.
9. Existir um Sink Connector funcional para um destino local ou facilmente implantável.
10. Os principais experimentos puderem ser repetidos.
11. As escolhas técnicas e resultados dos testes estiverem documentados.
12. Executar `deploy.sh` em um cluster OpenShift conectado via `oc`.
13. O ArgoCD instalar e sincronizar automaticamente todos os recursos.
14. Todo o estado estar declarado no repositório Git.
15. O `README.md` documentar como usar e validar.

---

## 20. Princípio central de implementação

**Não assuma decisões técnicas detalhadas apenas porque parecem óbvias.**

A IA responsável pela implementação deve:

1. Entender o objetivo funcional.
2. Identificar opções tecnicamente válidas.
3. Consultar documentação oficial e compatibilidade entre versões.
4. Testar hipóteses no ambiente.
5. Escolher a alternativa mais simples que cumpra o objetivo.
6. Documentar as decisões tomadas e o motivo.
7. Mudar de abordagem quando uma hipótese não funcionar bem.

Este arquivo define **o que a demo precisa demonstrar**, não exatamente **como cada componente deve ser implantado**.

Evitar introduzir complexidade que não contribua diretamente para o aprendizado.

---

## 21. Escopo funcional da demo

### 21.1 Red Hat Streams for Apache Kafka

Deve existir um cluster Kafka funcional utilizando **Red Hat Streams for Apache Kafka**.

Objetivos de aprendizado:

- Criar e administrar tópicos.
- Entender partições e replicação.
- Publicar e consumir eventos.
- Observar o comportamento básico do cluster.

A topologia exata, número de brokers, storage, listeners e demais detalhes devem ser escolhidos durante a implementação de acordo com o ambiente disponível e o objetivo educacional.

### 21.2 Aplicação Producer

Criar uma aplicação simples responsável por publicar eventos no Kafka.

Objetivos de aprendizado:

- Conexão com Kafka.
- Produção de eventos.
- Uso de chave quando apropriado.
- Serialização.
- Integração com o Registry quando aplicável.

A linguagem, framework e formato exatos devem ser escolhidos durante a implementação.

### 21.3 Aplicação Consumer

Criar uma aplicação simples responsável por consumir eventos do Kafka.

Objetivos de aprendizado:

- Conexão com Kafka.
- Consumer groups.
- Consumo de mensagens.
- Desserialização.
- Integração com o Registry quando aplicável.

Deve existir apenas a complexidade necessária para demonstrar o fluxo.

### 21.4 Kafka Connect

A demo deve utilizar **Kafka Connect**.

Objetivos de aprendizado:

- Entender a diferença entre Kafka Connect, Connector e Task.
- Observar como integrações externas são executadas.
- Entender Source Connector e Sink Connector.
- Visualizar configuração, status e operação de connectors.

Não criar integrações artificiais apenas para aumentar a quantidade de componentes.

### 21.5 CDC com Red Hat build of Debezium para PostgreSQL

Subir uma instância de **PostgreSQL** para servir como fonte de dados da demo.

Utilizar **Red Hat build of Debezium** para capturar mudanças no PostgreSQL e publicá-las no Kafka.

Fluxo esperado:

```text
PostgreSQL
    |
    | mudanças em dados
    v
Debezium / Kafka Connect
    |
    v
Kafka
```

A demo deve permitir executar operações simples no banco (INSERT, UPDATE, DELETE) e observar os eventos correspondentes chegando ao Kafka.

A versão do PostgreSQL, configuração específica do Debezium, estratégia de snapshot e detalhes de publicação devem ser decididos e testados durante a implementação.

### 21.6 CDC com Red Hat build of Debezium para Oracle

Subir uma instância de **Oracle Database** apropriada para a demo.

Utilizar **Red Hat build of Debezium** para capturar mudanças no Oracle e publicá-las no Kafka.

Fluxo esperado:

```text
Oracle Database
      |
      | mudanças em dados
      v
Debezium / Kafka Connect
      |
      v
Kafka
```

A demo deve permitir executar operações simples no banco e observar eventos CDC correspondentes.

Não fixar antecipadamente edição, imagem, estratégia de instalação ou configuração detalhada do Oracle. A IA executora deve escolher uma alternativa adequada para laboratório, validar compatibilidade e documentar a escolha.

### 21.7 Red Hat build of Apicurio Registry

A demo deve incluir **Red Hat build of Apicurio Registry**.

Objetivos de aprendizado:

- Registrar schemas.
- Versionar schemas.
- Associar aplicações a schemas registrados.
- Utilizar serializers e deserializers compatíveis.
- Observar regras de validade e compatibilidade.
- Testar uma evolução de schema compatível.
- Testar, quando tecnicamente apropriado, uma evolução incompatível e observar o comportamento resultante.

Não definir antecipadamente o modo de instalação, storage interno, banco utilizado pelo Registry ou modelo exato de integração. Essas decisões devem ser tomadas durante a implementação com base na documentação oficial, compatibilidade das versões e simplicidade operacional.

### 21.8 Data Contracts

A demo deve explorar **conceitos de Data Contracts** utilizando as capacidades disponíveis no stack Red Hat.

O objetivo não é presumir equivalência 1:1 com funcionalidades específicas de outras plataformas.

A implementação deve investigar e demonstrar, quando suportado e apropriado:

- Schema.
- Versionamento.
- Compatibilidade.
- Metadata.
- Labels ou mecanismos equivalentes.
- Regras de validade.
- Políticas de evolução.
- Validação realizada por producers, consumers ou outros componentes quando necessário.

A IA executora deve deixar explícito:

1. Quais partes do exercício são implementadas nativamente pelo Apicurio Registry.
2. Quais partes dependem da aplicação ou de outro mecanismo.
3. Quais conceitos de Data Contract não possuem representação direta no mecanismo escolhido.

Não inventar funcionalidades inexistentes no Apicurio.

### 21.9 Substituto educacional para o fluxo Kafka → armazenamento externo

O cenário real possui integração de saída do Kafka para Google Cloud Storage. **Google Cloud Storage não deve ser utilizado nesta demo.**

A demo deve demonstrar o conceito de um **Sink Connector**:

```text
Kafka
  |
  v
Sink Connector
  |
  v
Destino externo
```

A IA executora deve selecionar um destino local ou facilmente implantável que cumpra o papel educacional de armazenamento externo.

Ao escolher a alternativa:

- Priorizar tecnologias coerentes com o ecossistema Red Hat e OpenShift.
- Evitar dependências desnecessárias de cloud pública.
- Preferir uma solução simples de observar e validar.
- Explicar por que ela é uma boa analogia para o fluxo original.
- Não afirmar que ela é equivalente ao GCS em produção.

A escolha exata deve ser feita somente após avaliação e teste.

---

## 22. Componentes explicitamente fora do escopo

Os itens abaixo **não devem ser implementados nesta fase**:

- Confluent Cloud.
- Qualquer componente proprietário da Confluent.
- Google Cloud Pub/Sub.
- Google Cloud Storage.
- MirrorMaker 2.
- Migração real entre clusters.
- Coexistência Confluent e Red Hat.
- Estratégia de cutover.
- Sizing equivalente ao ambiente do cliente.
- Centenas de tópicos ou grandes volumes.
- Reprodução da topologia de rede corporativa.
- VPC peering.
- DNS corporativo.
- Hardening completo de produção.
- Alta disponibilidade em escala de produção.
- Disaster recovery completo.

Esses temas podem ser estudados posteriormente, mas não devem aumentar a complexidade desta demo.

---

## 23. Arquitetura conceitual esperada

A implementação final deverá se aproximar conceitualmente deste desenho, sem considerar o diagrama como especificação rígida:

```text
                           +----------------------+
                           |   Apicurio Registry  |
                           +----------+-----------+
                                      ^
                                      |
                         schemas / serialization
                                      |
                 +--------------------+--------------------+
                 |                                         |
           +-----+------+                           +------+------+
           |  Producer  |                           |  Consumer   |
           +-----+------+                           +------+------+
                 |                                         ^
                 |                                         |
                 +------------------> Kafka <---------------+
                                      ^
                                      |
                       +--------------+--------------+
                       |                             |
                +------+-------+              +------+-------+
                | Kafka Connect|              | Kafka Connect|
                | + Debezium   |              | + Debezium   |
                +------+-------+              +------+-------+
                       ^                             ^
                       |                             |
                +------+-------+              +------+-------+
                | PostgreSQL   |              |    Oracle    |
                +--------------+              +--------------+

                                      |
                                      v

                                Sink Connector
                                      |
                                      v
                         armazenamento local escolhido
```

A arquitetura real poderá variar se testes mostrarem uma opção mais adequada.

---

## 24. Quantidade de aplicações e tópicos

Manter a demo pequena.

Aplicações obrigatórias:

- 1 producer.
- 1 consumer.

Criar somente os tópicos necessários para demonstrar:

- Eventos produzidos pela aplicação.
- CDC de PostgreSQL.
- CDC de Oracle.
- Integração de saída por Sink Connector.
- Schemas e evolução de schemas.

Não existe meta de quantidade de tópicos. A regra é: **usar o menor número que permita demonstrar os conceitos de forma clara**.

---

## 25. Cenário de dados sugerido

Pode ser utilizado um domínio simples e compreensível, como:

- Clientes.
- Pagamentos.
- Pedidos.
- Autorizações.

A IA executora pode escolher o domínio que produzir a implementação mais clara.

O cenário deve permitir demonstrar:

- Criação de uma entidade.
- Alteração.
- Remoção.
- Publicação de eventos.
- Consumo.
- CDC.
- Schema.
- Evolução de schema.

Evitar modelagem de negócio complexa.

---

## 26. Experimentos mínimos esperados

Ao final da demo, deve ser possível executar uma sequência equivalente a:

### Experimento A: Kafka básico

1. Iniciar o producer.
2. Publicar um evento.
3. Observar o evento no tópico.
4. Consumir o evento pela aplicação consumer.

### Experimento B: Partições e chaves

Demonstrar de forma simples o efeito de chaves e partições, sem transformar isso em benchmark.

### Experimento C: PostgreSQL CDC

1. Inserir registro no PostgreSQL.
2. Observar evento Debezium no Kafka.
3. Alterar o registro.
4. Observar novo evento.
5. Remover o registro.
6. Observar o evento correspondente.

### Experimento D: Oracle CDC

Repetir um fluxo equivalente utilizando Oracle.

### Experimento E: Schema Registry

1. Registrar um schema.
2. Produzir e consumir eventos utilizando o Registry.
3. Criar nova versão.
4. Observar compatibilidade e versionamento.

### Experimento F: Data Contract

Demonstrar, de forma compatível com as capacidades reais do stack escolhido:

- Estrutura do dado.
- Política de compatibilidade.
- Metadata ou mecanismo equivalente.
- Pelo menos uma restrição ou política relevante.
- Onde essa restrição é efetivamente aplicada.

### Experimento G: Sink Connector

1. Produzir eventos no Kafka.
2. Executar um Sink Connector.
3. Verificar que os dados foram persistidos no destino externo escolhido.

---

## 27. Requisitos de documentação da demo

Durante a construção da demo, documentar:

- Versões utilizadas.
- Fontes oficiais consultadas.
- Hipóteses testadas.
- Decisões tomadas.
- Alternativas descartadas.
- Incompatibilidades encontradas.
- Comandos e manifestos necessários para reproduzir o ambiente.
- Problemas encontrados e respectivas soluções.
- Limitações da demo.
- Diferenças conhecidas entre laboratório e produção.

Não esconder tentativas que falharam quando elas forem relevantes para explicar a decisão final.

---

## 28. Diretrizes para escolha de versões

Não selecionar versões arbitrariamente.

Antes de instalar os componentes, validar compatibilidade entre:

- OpenShift.
- Red Hat Streams for Apache Kafka.
- Red Hat build of Debezium.
- Red Hat build of Apicurio Registry.
- Oracle Database.
- PostgreSQL.
- Bibliotecas Kafka utilizadas pelo producer e consumer.

Priorizar documentação oficial da Red Hat e documentação oficial dos produtos envolvidos.

Quando houver mais de uma combinação suportada, escolher a alternativa que minimize complexidade para laboratório.

---

## 29. Observabilidade

Adicionar somente o nível de observabilidade necessário para compreender e depurar os fluxos.

Deve ser possível verificar pelo menos:

- Saúde dos componentes.
- Status dos connectors.
- Existência dos tópicos.
- Chegada de mensagens.
- Falhas básicas.

Não transformar esta fase em uma demo de observabilidade.

---

## 30. Filosofia de simplificação

Quando houver dúvida entre duas opções, favorecer a que:

1. Possua suporte/documentação oficial adequada.
2. Seja mais simples de reproduzir.
3. Torne o comportamento visível.
4. Ensine melhor o conceito.
5. Introduza menos infraestrutura irrelevante.

O objetivo é aprender Kafka, Connect, Debezium, Apicurio e os fluxos entre eles. Não é construir uma miniatura perfeita do ambiente corporativo.

---

## 31. Questões que a IA executora deve resolver experimentalmente

Estas perguntas são deliberadamente deixadas em aberto:

- Qual é a melhor forma de implantar o Apicurio Registry neste laboratório?
- Qual combinação de versões é mais adequada?
- Qual formato de schema torna o exercício mais didático?
- Qual tecnologia deve ser usada pelo producer e consumer?
- Qual é a melhor forma de representar e testar conceitos de Data Contract no stack Red Hat disponível?
- Qual destino deve substituir educacionalmente o Google Cloud Storage?
- Qual Sink Connector deve ser utilizado para esse destino?
- Qual abordagem de Oracle é mais simples e compatível para laboratório?
- Como organizar Kafka Connect e os connectors?
- Quantos tópicos são realmente necessários?
- Quais configurações de partição e replicação melhor demonstram os conceitos sem criar complexidade desnecessária?

A IA executora deve responder essas perguntas com pesquisa, testes e evidências, não apenas por preferência.

---

## 32. Não objetivos

Não otimizar esta demo para:

- Benchmark.
- Performance máxima.
- Custo real de produção.
- Sizing comercial.
- Migração de dados em escala.
- Equivalência funcional completa com Confluent.
- Arquitetura final do cliente.

Qualquer conclusão sobre produção deve ser tratada separadamente.

---

## 33. Registro de decisões e resultados da implementação

### 33.1 Versões utilizadas

| Componente | Versão | Observação |
|---|---|---|
| OpenShift | 4.22.9 | Cluster RHPDS |
| AMQ Streams (operator) | 3.2.1 | Canal `stable` |
| Apache Kafka (via AMQ Streams) | 4.2.0 | KRaft mode (sem ZooKeeper) |
| Red Hat build of Apicurio Registry | 3.2.6.redhat-00001 | Canal `3.x`, storage KafkaSQL |
| Red Hat build of Debezium | 3.6.1.Final | PostgreSQL + Oracle + JDBC Sink |
| Oracle JDBC driver (ojdbc11) | 23.6.0.24.10 | Para Debezium Oracle connector |
| Oracle Database Free | 23c (23-slim) | `gvenzl/oracle-free:23-slim` |
| PostgreSQL | 12-el8 | Imagem interna OpenShift |
| Quarkus | 3.38.3 | Producer e Consumer apps |
| Java | 21 | `ubi8-openjdk-21:1.18` (S2I) |
| ArgoCD | Instalado via deploy.sh | Auto-sync + self-heal + prune |

### 33.2 Decisões técnicas relevantes

**Kafka em modo KRaft:** O AMQ Streams 3.2 suporta nativamente KRaft (sem ZooKeeper), usando KafkaNodePool com roles `[controller, broker]` em 3 nós combinados. Reduz a complexidade operacional.

**Apicurio Registry com KafkaSQL:** Armazena os schemas diretamente no Kafka (tópicos `kafkasql-journal` e `kafkasql-snapshots`), eliminando a necessidade de banco de dados externo para o Registry.

**Oracle Free 23c (não XE):** Oracle XE não suporta supplemental logging necessário para Debezium LogMiner. Oracle Database Free 23c é a edição mais leve que suporta CDC. Requer `anyuid` SCC no OpenShift (UID 54321).

**Debezium Oracle via LogMiner:** Usa common user `C##DBZUSER` no CDB para acessar redo logs. Requer ARCHIVELOG mode + supplemental logging (ALL COLUMNS).

**JDBC Sink em vez de MinIO/S3:** O AGENTS.md pedia um substituto educacional para GCS. O Debezium JDBC Sink Connector (parte do stack Red Hat) reutiliza o PostgreSQL existente e demonstra o conceito de Sink Connector sem infraestrutura adicional. A analogia: assim como GCS armazena dados de forma durável fora do Kafka, o Sink escreve em PostgreSQL — o conceito de "destino externo" é o mesmo.

**JsonConverter com schemas.enable:** Os source connectors precisam de `schemas.enable: true` nos converters para que o JDBC Sink consiga criar tabelas automaticamente (`schema.evolution: basic`).

**KafkaConnector CRs obrigatórios:** Com `strimzi.io/use-connector-resources: "true"`, o Strimzi remove conectores criados via REST API que não têm CRs correspondentes. Todos os conectores precisam de CRs.

**Debezium 3.6.1 + Kafka Connect 4.2.0:** Incompatibilidade de validação gera `NullPointerException` no endpoint PUT do REST API. Workaround: criar conectores via POST ou diretamente via KafkaConnector CRs (que usam um caminho diferente). Os conectores funcionam normalmente após criação.

### 33.3 Problemas encontrados e soluções

1. **PostgreSQL init scripts:** ConfigMaps montados como init scripts interferiram no processo de inicialização do container PostgreSQL. Solução: usar apenas `custom.conf` para `wal_level=logical` e criar tabelas manualmente.

2. **PostgreSQL replication role:** O usuário `demouser` precisa de `ALTER ROLE demouser REPLICATION` para que o Debezium crie replication slots.

3. **PVC órfão bloqueando sync:** Um PVC `postgresql-data` de uma configuração anterior bloqueou o sync do ArgoCD. Solução: deletar manualmente e o auto-sync prosseguiu.

4. **Oracle ARCHIVELOG:** Oracle Free 23c inicia em NOARCHIVELOG mode. Requer `SHUTDOWN IMMEDIATE → STARTUP MOUNT → ALTER DATABASE ARCHIVELOG → ALTER DATABASE OPEN`.

### 33.4 Experimentos validados

| Experimento | Status | Resultado |
|---|---|---|
| A: Kafka básico (produce/consume) | ✅ | Producer envia via REST, Consumer recebe e armazena |
| B: Partições e chaves | ✅ | 3 partições, replicação factor 3, distribuição por key hash |
| C: PostgreSQL CDC | ✅ | INSERT, UPDATE, DELETE capturados via Debezium + pgoutput |
| D: Oracle CDC | ✅ | INSERT, UPDATE, DELETE capturados via Debezium LogMiner |
| E: Schema Registry | ✅ | Schemas registrados, versionados, regras de compatibilidade ativas |
| F: Data Contract | ✅ | Schemas + metadata/labels + políticas BACKWARD + validação |
| G: Sink Connector | ✅ | Oracle CDC → Kafka → JDBC Sink → PostgreSQL (`oracle_customers`) |

### 33.5 Conceitos de Data Contract demonstrados

O Apicurio Registry oferece nativamente:
- **Schema:** Definição estrutural (JSON Schema) do evento
- **Versionamento:** Múltiplas versões do mesmo artifact (v1.0.0, v1.1.0, v2.0.0)
- **Compatibilidade:** Regras BACKWARD, FORWARD, FULL, NONE por artifact
- **Metadata/Labels:** `contract-owner`, `data-classification`, `sla-availability`, `kafka-topic`, `producer-app`, `consumer-apps`
- **Validação:** SYNTAX_ONLY ou FULL validity check no registro

O que NÃO é nativo do Apicurio:
- Enforcement automático no broker (Kafka não valida mensagens contra o Registry)
- SLA monitoring integrado
- Lineage/proveniência de dados
- Políticas de acesso por aplicação

A validação efetiva depende dos serializers/deserializers nas aplicações (Apicurio serde libraries) ou de mecanismos externos.

### 33.6 Arquitetura final implantada

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
           +------------------+  +--+-----> Sink Connector
           |                     |         (JDBC → PostgreSQL)
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

### 33.7 URLs de acesso

- **Producer API:** `https://producer.apps.<domain>/api/orders` (POST)
- **Consumer API:** `https://consumer.apps.<domain>/api/orders` (GET) + `/api/orders/count`
- **Apicurio Registry API:** `https://apicurio-api.apps.<domain>/apis/registry/v3/`
- **Apicurio Registry UI:** `https://apicurio-ui.apps.<domain>/`
