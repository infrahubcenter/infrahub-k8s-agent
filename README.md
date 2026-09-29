# InfraHub Kubernetes Agent

In-cluster agent that dials out to an InfraHub backend over a WebSocket
and reports read-only cluster metrics/logs. Install this once inside any
Kubernetes cluster you want InfraHub to monitor.

## Install

1. In InfraHub, go to **Kubernetes Clusters** and click **Connect Cluster**.
   It gives you a one-time agent token (shown once, like a GitHub PAT) and
   the backend URL to use.
2. Create the agent's Secret with your real values:

   ```bash
   kubectl create namespace infrahub-agent
   kubectl create secret generic infrahub-k8s-agent -n infrahub-agent \
     --from-literal=backend-url=<the ws(s)://... URL InfraHub showed you> \
     --from-literal=agent-token=<the token InfraHub showed you>
   ```

3. Apply the manifest:

   ```bash
   kubectl apply -f https://raw.githubusercontent.com/infrahubcenter/infrahub-k8s-agent/main/deploy/manifest.yaml
   ```

The agent only ever dials **out** to InfraHub; nothing needs to be exposed
or opened on this cluster for it to work. Its ServiceAccount is granted
read-only (get/list/watch) access to a broad set of resource kinds --
pods, pods/log, nodes, namespaces, deployments/statefulsets/daemonsets/
replicasets, jobs/cronjobs, services, persistentvolumeclaims/
persistentvolumes/storageclasses, ingresses/networkpolicies,
endpointslices, resourcequotas/limitranges, poddisruptionbudgets, and
horizontalpodautoscalers -- plus read access to pod/node metrics if
metrics-server is installed. See `deploy/manifest.yaml`'s own header
comment for the exact, current RBAC rules (kept accurate there, not
duplicated here to avoid drift). It deliberately has no access to
Secrets, ConfigMaps, or any RBAC object, and nothing in the manifest can
create, modify, or delete any cluster resource.

If you already have an older version of this agent installed, re-run
`kubectl apply -f manifest.yaml` after an update to pick up any RBAC
changes -- see the manifest's own "IMPORTANT" note for details.

## Building your own image

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t <your-registry>/k8s-agent:<tag> --push .
```

Then update `deploy/manifest.yaml`'s `image:` field to point at it.
