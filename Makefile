# Scout on Kubernetes (kind). Needs docker, kind, kubectl and helm on PATH.
#
#   make kind-up      create the cluster, build and load the image, install the chart, wait until
#                     the search API is ready (http://127.0.0.1:8080/v1/readyz)
#   make kind-smoke   check the running release: helm test, searches, scaling workers up, a worker
#                     deleted mid-crawl
#   make kind-down    delete the cluster
#   make helm-lint    lint the chart and validate what it renders (kubeconform, if installed)
#
# Variables: CLUSTER, NAMESPACE, RELEASE, IMAGE_REPO, IMAGE_TAG, BUILD_IMAGE=0 (use an image that
# is already built), HELM_ARGS (extra `helm upgrade --install` flags).

CLUSTER    ?= scout
NAMESPACE  ?= scout
RELEASE    ?= scout
IMAGE_REPO ?= scout
IMAGE_TAG  ?= kind
BUILD_IMAGE ?= 1
HELM_ARGS  ?=

CHART   := deploy/helm/scout
KUBECTL := kubectl --context kind-$(CLUSTER) -n $(NAMESPACE)
HELM    := helm --kube-context kind-$(CLUSTER) -n $(NAMESPACE)

.PHONY: kind-up kind-down kind-deploy kind-smoke kind-diagnose kind-metrics-server helm-lint

kind-up:
	kind get clusters | grep -qx '$(CLUSTER)' || kind create cluster --config deploy/kind/cluster.yaml --name $(CLUSTER)
	[ '$(BUILD_IMAGE)' = 0 ] || docker build -t $(IMAGE_REPO):$(IMAGE_TAG) .
	kind load docker-image $(IMAGE_REPO):$(IMAGE_TAG) --name $(CLUSTER)
	$(MAKE) --no-print-directory kind-deploy
	$(KUBECTL) rollout status statefulset/$(RELEASE)-postgres --timeout=5m || { $(MAKE) --no-print-directory kind-diagnose; exit 1; }
	$(KUBECTL) rollout status deploy/$(RELEASE)-coordinator --timeout=3m || { $(MAKE) --no-print-directory kind-diagnose; exit 1; }
	# Ready once the crawl has finished and the index Job has activated a Generation.
	$(KUBECTL) rollout status deploy/$(RELEASE)-serve --timeout=10m || { $(MAKE) --no-print-directory kind-diagnose; exit 1; }
	@echo "scout is up: curl http://127.0.0.1:8080/v1/readyz"

# Install or upgrade the release. The run ID defaults to the chart's; pass
# HELM_ARGS='--set crawl.runId=run-<64 hex>' to crawl again.
kind-deploy:
	$(HELM) upgrade --install $(RELEASE) $(CHART) --create-namespace \
		-f deploy/kind/values.yaml \
		--set image.repository=$(IMAGE_REPO) --set image.tag=$(IMAGE_TAG) $(HELM_ARGS)

kind-smoke:
	CLUSTER=$(CLUSTER) NAMESPACE=$(NAMESPACE) RELEASE=$(RELEASE) \
		IMAGE_REPO=$(IMAGE_REPO) IMAGE_TAG=$(IMAGE_TAG) scripts/ci/kind-smoke.sh

# Pod state, events and every container's logs, for when something does not come up.
kind-diagnose:
	CLUSTER=$(CLUSTER) NAMESPACE=$(NAMESPACE) scripts/ci/kind-diagnose.sh

kind-down:
	kind delete cluster --name $(CLUSTER)

# Needed by worker.autoscaling.enabled=true (the HPA reads CPU from it).
kind-metrics-server:
	kubectl --context kind-$(CLUSTER) apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml
	kubectl --context kind-$(CLUSTER) -n kube-system patch deployment metrics-server --type=json \
		-p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
	kubectl --context kind-$(CLUSTER) -n kube-system rollout status deployment/metrics-server --timeout=3m

helm-lint:
	helm lint $(CHART)
	helm lint $(CHART) --set fixture.mode=synthetic --set worker.autoscaling.enabled=true
	@if command -v kubeconform >/dev/null; then \
		for flags in "" "--set fixture.mode=synthetic --set worker.autoscaling.enabled=true"; do \
			helm template $(RELEASE) $(CHART) $$flags | kubeconform -strict -summary -kubernetes-version 1.32.0; \
		done; \
	else echo "kubeconform not installed: skipping schema validation"; fi
