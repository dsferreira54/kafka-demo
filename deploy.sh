#!/bin/bash

set -euo pipefail

REPO_URL="$(git remote get-url origin 2>/dev/null)"
if [ -z "$REPO_URL" ]; then
  echo "Error: could not determine the repository URL from git remote."
  echo "Make sure you are running this script from inside a cloned git repository."
  exit 1
fi

APP_NAME="$(basename -s .git "$REPO_URL")"

echo "Repository : ${REPO_URL}"
echo "App name   : ${APP_NAME}"

INGRESS_DOMAIN="$(oc get ingresses.config.openshift.io cluster -o jsonpath='{.spec.domain}')"
echo "Ingress    : ${INGRESS_DOMAIN}"

echo ""
echo "Installing OpenShift GitOps operator..."

oc apply -f - <<'EOF'
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: openshift-gitops-operator
  namespace: openshift-operators
spec:
  channel: latest
  installPlanApproval: Automatic
  name: openshift-gitops-operator
  source: redhat-operators
  sourceNamespace: openshift-marketplace
EOF

echo "Waiting for ArgoCD to become Available..."

until [ "$(oc get argocd openshift-gitops -n openshift-gitops -o jsonpath='{.status.phase}' 2>/dev/null)" = "Available" ]; do
  sleep 10
done

echo "ArgoCD is Available."

oc patch argocd openshift-gitops \
  -n openshift-gitops \
  --type merge \
  -p '{
    "spec": {
      "controller": {
        "appSync": "5s"
      },
      "extraConfig": {
        "timeout.reconciliation.jitter": "0s"
      }
    }
  }'

echo "Granting cluster-admin to the ArgoCD application controller..."

oc apply -f - <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: openshift-gitops-argocd-application-controller-cluster-admin
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: openshift-gitops-argocd-application-controller
  namespace: openshift-gitops
EOF

echo "Creating ArgoCD Application '${APP_NAME}'..."

oc apply -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ${APP_NAME}
  namespace: openshift-gitops
spec:
  project: default
  source:
    repoURL: ${REPO_URL}
    targetRevision: main
    path: gitops
    helm:
      parameters:
        - name: ingressDomain
          value: "${INGRESS_DOMAIN}"
        - name: repoUrl
          value: "${REPO_URL}"
  destination:
    server: https://kubernetes.default.svc
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - ApplyOutOfSyncOnly=true
      - SkipDryRunOnMissingResource=true
      - CreateNamespace=true
    retry:
      limit: -1
      backoff:
        duration: 10s
        factor: 2
        maxDuration: 3m
EOF

ARGOCD_ADMIN_PASSWORD="$(oc get secret openshift-gitops-cluster -n openshift-gitops -o jsonpath='{.data.admin\.password}' 2>/dev/null | base64 -d)"

echo ""
echo "========================================"
echo "  Deployment complete"
echo "========================================"
echo ""
echo "  App name : ${APP_NAME}"
echo "  Repo     : ${REPO_URL}"
echo ""
echo "  ArgoCD credentials:"
echo "    Username : admin"
if [ -n "$ARGOCD_ADMIN_PASSWORD" ]; then
  echo "    Password : ${ARGOCD_ADMIN_PASSWORD}"
fi
ARGOCD_URL="$(oc get route openshift-gitops-server -n openshift-gitops -o jsonpath='{.spec.host}' 2>/dev/null)"
if [ -n "$ARGOCD_URL" ]; then
  echo "    URL      : https://${ARGOCD_URL}"
fi
echo ""
