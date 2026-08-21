# Demo Template — OpenShift GitOps

Template de modo de trabalho para demos em Red Hat OpenShift com GitOps (ArgoCD + Helm).

## Como usar este template

1. **Fork** este repositório no GitHub.
2. **Clone** o fork:

```bash
git clone https://github.com/<seu-usuario>/<nome-do-fork>.git
cd <nome-do-fork>
```

3. **Edite o `AGENTS.md`** descrevendo o objetivo da demo, as fases do projeto e os critérios de sucesso.

4. **Conecte-se ao cluster OpenShift** via `oc`:

```bash
oc login --server=https://api.your-cluster.example.com:6443
```

5. **Execute o deploy**:

```bash
bash deploy.sh
```

O script automaticamente:
- Detecta a URL do repositório via `git remote`
- Instala o operador OpenShift GitOps (ArgoCD)
- Cria uma Application apontando para o diretório `gitops/` do repositório
- Exibe as credenciais e URL do ArgoCD

6. **Desenvolva a demo** adicionando manifestos Kubernetes em `gitops/templates/`. O ArgoCD sincroniza automaticamente.

## Pré-requisitos

- Cluster OpenShift 4.x com acesso `cluster-admin`
- `oc` CLI instalado e autenticado
- Repositório público no GitHub (sem necessidade de autenticação no ArgoCD)

## Estrutura do repositório

```
├── AGENTS.md              # Contexto e regras para agentes de IA
├── deploy.sh              # Bootstrap do ArgoCD (auto-detecta repo URL)
├── README.md              # Este arquivo
├── .gitignore
├── apps/                  # Aplicações auxiliares (opcional)
└── gitops/
    ├── Chart.yaml
    ├── .helmignore
    ├── values.yaml
    └── templates/         # Manifestos Kubernetes (gerenciados pelo ArgoCD)
```

## Como funciona

```
git push → GitHub → ArgoCD (sync automático) → OpenShift cluster
```

- Todo o estado do cluster é declarado em `gitops/templates/`.
- O ArgoCD monitora o repositório e aplica as alterações automaticamente.
- Alterações manuais via `oc` são permitidas para prototipação, mas devem ser transcritas para o Helm chart.

## Modo de trabalho com IA

Este template foi projetado para trabalho assistido por IA:

1. Preencha o `AGENTS.md` com o contexto da demo.
2. Peça à IA para implementar as fases descritas.
3. A IA fará commits incrementais, validará no cluster e documentará no README.
4. O resultado final será um repositório Git com todo o estado declarado e documentado.
