// k8sClient wraps the in-cluster Kubernetes API access this agent uses to
// answer commands -- always its own ServiceAccount's credentials
// (rest.InClusterConfig()), never anything supplied by InfraHub's
// backend. InfraHub never sees these credentials or any client-go
// internals; only the safe, small PodInfo/ServerVersionResult shapes
// defined in protocol.go ever leave this process.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"
)

type k8sClient struct {
	clientset *kubernetes.Clientset
	// metrics is nil when metrics-server isn't installed or reachable --
	// every caller treats that as "no metrics available" (nil CPU/Memory
	// fields), never a fatal error.
	metrics *metricsclientset.Clientset
}

func newK8sClient() (*k8sClient, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster config (is this running inside a pod?): %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	metrics, err := metricsclientset.NewForConfig(cfg)
	if err != nil {
		log.Printf("metrics-server client unavailable, pod/node CPU/memory usage figures will be omitted: %v", err)
		metrics = nil
	}
	return &k8sClient{clientset: clientset, metrics: metrics}, nil
}

// ServerVersion answers the "server_version" command.
func (c *k8sClient) ServerVersion(ctx context.Context) (string, error) {
	_ = ctx // client-go's Discovery client has no context-aware variant here
	v, err := c.clientset.Discovery().ServerVersion()
	if err != nil {
		return "", err
	}
	return v.String(), nil
}

// ListPods answers the "list_pods" command -- namespace == "" lists every
// namespace. Metrics are always best-effort: a pod simply has nil
// CPU/Memory fields when metrics-server isn't installed, never a
// fabricated value.
func (c *k8sClient) ListPods(ctx context.Context, namespace string) ([]PodInfo, error) {
	pods, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	type usage struct{ cpuMillicores, memoryBytes int64 }
	metricsByPod := map[string]usage{}
	if c.metrics != nil {
		if podMetrics, err := c.metrics.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{}); err == nil {
			for _, pm := range podMetrics.Items {
				var u usage
				for _, cont := range pm.Containers {
					if q := cont.Usage.Cpu(); q != nil {
						u.cpuMillicores += q.MilliValue()
					}
					if q := cont.Usage.Memory(); q != nil {
						u.memoryBytes += q.Value()
					}
				}
				metricsByPod[pm.Namespace+"/"+pm.Name] = u
			}
		}
	}

	result := make([]PodInfo, 0, len(pods.Items))
	fullyReady := 0
	for _, pod := range pods.Items {
		var ready, total, restarts int32
		for _, cs := range pod.Status.ContainerStatuses {
			total++
			if cs.Ready {
				ready++
			}
			restarts += cs.RestartCount
		}
		info := PodInfo{
			Namespace: pod.Namespace, PodName: pod.Name, NodeName: pod.Spec.NodeName,
			Phase: string(pod.Status.Phase), ReadyContainers: ready, TotalContainers: total, RestartCount: restarts,
		}
		if pod.Status.StartTime != nil {
			info.StartedAt = pod.Status.StartTime.Time.UTC().Format(time.RFC3339)
		}
		if u, ok := metricsByPod[pod.Namespace+"/"+pod.Name]; ok {
			cpu, mem := u.cpuMillicores, u.memoryBytes
			info.CPUMillicores, info.MemoryBytes = &cpu, &mem
		}
		if total > 0 && ready == total {
			fullyReady++
		}
		result = append(result, info)
	}
	ns := namespace
	if ns == "" {
		ns = "all namespaces"
	}
	log.Printf("k8s discovery: %d pods in %s (%d fully ready)", len(result), ns, fullyReady)
	return result, nil
}

// FetchLogsSince answers the "fetch_logs_since" command -- a single
// bounded, non-follow read of everything logged at or after since
// (the zero time means "from the beginning of the container's current log
// buffer"). Every line is timestamped, matching the backend's expected
// `<RFC3339Nano> <text>` capture format exactly.
func (c *k8sClient) FetchLogsSince(ctx context.Context, namespace, podName string, since time.Time) (string, error) {
	opts := &corev1.PodLogOptions{Timestamps: true}
	if !since.IsZero() {
		t := metav1.NewTime(since)
		opts.SinceTime = &t
	}
	stream, err := c.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ListNodes answers the "list_nodes" command -- every node's capacity/
// allocatable figures plus best-effort usage (metrics-server for CPU/
// memory, the kubelet's own stats/summary proxy for storage) and pod
// count. Never fails just because usage figures aren't available -- only
// the base node List/Pods List calls (both required) can fail this call
// outright.
func (c *k8sClient) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	podCountByNode := map[string]int32{}
	if pods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{}); err == nil {
		for _, p := range pods.Items {
			if p.Spec.NodeName != "" {
				podCountByNode[p.Spec.NodeName]++
			}
		}
	}

	type usage struct{ cpuMillicores, memoryBytes int64 }
	usageByNode := map[string]usage{}
	if c.metrics != nil {
		if nodeMetrics, err := c.metrics.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{}); err == nil {
			for _, nm := range nodeMetrics.Items {
				var u usage
				if q := nm.Usage.Cpu(); q != nil {
					u.cpuMillicores = q.MilliValue()
				}
				if q := nm.Usage.Memory(); q != nil {
					u.memoryBytes = q.Value()
				}
				usageByNode[nm.Name] = u
			}
		}
	}

	result := make([]NodeInfo, 0, len(nodes.Items))
	readyCount, storageMissing := 0, 0
	for _, node := range nodes.Items {
		info := NodeInfo{
			Name: node.Name, KubeletVersion: node.Status.NodeInfo.KubeletVersion, OSImage: node.Status.NodeInfo.OSImage,
			PodCount: podCountByNode[node.Name],
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady {
				info.Ready = cond.Status == corev1.ConditionTrue
				break
			}
		}
		for label := range node.Labels {
			if role, ok := strings.CutPrefix(label, "node-role.kubernetes.io/"); ok {
				info.Roles = append(info.Roles, role)
			}
		}
		if q, ok := node.Status.Capacity[corev1.ResourceCPU]; ok {
			info.CPUCapacityMillicores = q.MilliValue()
		}
		if q, ok := node.Status.Allocatable[corev1.ResourceCPU]; ok {
			info.CPUAllocatableMillicores = q.MilliValue()
		}
		if q, ok := node.Status.Capacity[corev1.ResourceMemory]; ok {
			info.MemoryCapacityBytes = q.Value()
		}
		if q, ok := node.Status.Allocatable[corev1.ResourceMemory]; ok {
			info.MemoryAllocatableBytes = q.Value()
		}
		if q, ok := node.Status.Capacity[corev1.ResourcePods]; ok {
			info.PodCapacity = q.Value()
		}
		if u, ok := usageByNode[node.Name]; ok {
			cpu, mem := u.cpuMillicores, u.memoryBytes
			info.CPUUsageMillicores, info.MemoryUsageBytes = &cpu, &mem
		}
		if capBytes, usedBytes, ok := c.nodeStorageStats(ctx, node.Name); ok {
			info.StorageCapacityBytes, info.StorageUsageBytes = &capBytes, &usedBytes
		} else {
			storageMissing++
		}
		if info.Ready {
			readyCount++
		}
		result = append(result, info)
	}
	log.Printf("k8s discovery: %d nodes (%d ready)", len(result), readyCount)
	if storageMissing > 0 {
		log.Printf("k8s discovery: storage stats unavailable for %d/%d nodes (kubelet stats/summary proxy)", storageMissing, len(result))
	}
	return result, nil
}

// nodeStorageStats is best-effort: the kubelet's stats/summary endpoint is
// reached through the API server's node proxy (requires the "nodes/proxy"
// RBAC verb -- see deploy/manifest.yaml). Any failure (RBAC not granted,
// node unreachable, unexpected response shape) simply means no storage
// figures for this one node, never a fatal error for the whole ListNodes
// call.
func (c *k8sClient) nodeStorageStats(ctx context.Context, nodeName string) (capacityBytes, usedBytes int64, ok bool) {
	raw, err := c.clientset.CoreV1().RESTClient().Get().
		Resource("nodes").Name(nodeName).SubResource("proxy").Suffix("stats/summary").
		Do(ctx).Raw()
	if err != nil {
		return 0, 0, false
	}
	var summary struct {
		Node struct {
			Fs struct {
				CapacityBytes *int64 `json:"capacityBytes"`
				UsedBytes     *int64 `json:"usedBytes"`
			} `json:"fs"`
		} `json:"node"`
	}
	if err := json.Unmarshal(raw, &summary); err != nil {
		return 0, 0, false
	}
	if summary.Node.Fs.CapacityBytes == nil || summary.Node.Fs.UsedBytes == nil {
		return 0, 0, false
	}
	return *summary.Node.Fs.CapacityBytes, *summary.Node.Fs.UsedBytes, true
}

// logListErr records that one resource kind's List() failed inside
// ClusterResourceSummary, without failing the whole summary -- see that
// function's own doc comment for why. Also appends to summary.FailedKinds
// so the backend's discovery sweep can tell "this kind genuinely has zero
// resources" apart from "this kind's fetch failed" -- without that, a
// transient RBAC/API error would look identical to real emptiness, and
// the sweep would incorrectly mark every existing resource of that kind
// as removed. Kept as a named helper rather than inlined so every one of
// the ~20 call sites logs (and records) identically.
func logListErr(summary *ClusterResourceSummary, kind string, err error) {
	log.Printf("k8s discovery: list %s failed (RBAC not granted for this kind, or agent not upgraded to match manifest.yaml yet?): %v", kind, err)
	summary.FailedKinds = append(summary.FailedKinds, kind)
}

// ClusterResourceSummary answers the "cluster_resource_summary" command --
// cluster-wide counts of exactly the resource kinds this agent's RBAC (see
// deploy/manifest.yaml) grants read access to. Each kind's List() call is
// independent: one kind failing (RBAC not yet upgraded to match this
// agent version, or a kind that doesn't exist on an older cluster, e.g.
// EndpointSlice) only zeroes that one kind's count/items via logListErr,
// never the rest of the summary. This matters much more at ~20 kinds than
// it did at the original 6 -- a single missing RBAC rule used to be rare
// enough that failing loudly was reasonable; at this scale it's the
// expected steady state for at least some cluster/RBAC-version
// combinations.
func (c *k8sClient) ClusterResourceSummary(ctx context.Context) (ClusterResourceSummary, error) {
	var summary ClusterResourceSummary

	if namespaces, err := c.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "namespaces", err)
	} else {
		summary.Namespaces = int32(len(namespaces.Items))
		summary.NamespaceItems = make([]NamespaceItem, 0, len(namespaces.Items))
		for _, ns := range namespaces.Items {
			summary.NamespaceItems = append(summary.NamespaceItems, NamespaceItem{Name: ns.Name, Status: string(ns.Status.Phase)})
		}
	}

	if nodes, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "nodes", err)
	} else {
		summary.Nodes = int32(len(nodes.Items))
	}

	if pods, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "pods", err)
	} else {
		summary.Pods = int32(len(pods.Items))
	}

	if deployments, err := c.clientset.AppsV1().Deployments("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "deployments", err)
	} else {
		summary.Deployments = int32(len(deployments.Items))
		summary.DeploymentItems = make([]WorkloadItem, 0, len(deployments.Items))
		for _, d := range deployments.Items {
			desired := int32(1)
			if d.Spec.Replicas != nil {
				desired = *d.Spec.Replicas
			}
			summary.DeploymentItems = append(summary.DeploymentItems, WorkloadItem{
				Name: d.Name, Namespace: d.Namespace, DesiredReplicas: desired, ReadyReplicas: d.Status.ReadyReplicas,
			})
		}
	}

	if statefulSets, err := c.clientset.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "statefulsets", err)
	} else {
		summary.StatefulSets = int32(len(statefulSets.Items))
		summary.StatefulSetItems = make([]WorkloadItem, 0, len(statefulSets.Items))
		for _, s := range statefulSets.Items {
			desired := int32(1)
			if s.Spec.Replicas != nil {
				desired = *s.Spec.Replicas
			}
			summary.StatefulSetItems = append(summary.StatefulSetItems, WorkloadItem{
				Name: s.Name, Namespace: s.Namespace, DesiredReplicas: desired, ReadyReplicas: s.Status.ReadyReplicas,
			})
		}
	}

	if daemonSets, err := c.clientset.AppsV1().DaemonSets("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "daemonsets", err)
	} else {
		summary.DaemonSets = int32(len(daemonSets.Items))
		summary.DaemonSetItems = make([]WorkloadItem, 0, len(daemonSets.Items))
		for _, ds := range daemonSets.Items {
			summary.DaemonSetItems = append(summary.DaemonSetItems, WorkloadItem{
				Name: ds.Name, Namespace: ds.Namespace,
				DesiredReplicas: ds.Status.DesiredNumberScheduled, ReadyReplicas: ds.Status.NumberReady,
			})
		}
	}

	if replicaSets, err := c.clientset.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "replicasets", err)
	} else {
		summary.ReplicaSets = int32(len(replicaSets.Items))
		summary.ReplicaSetItems = make([]WorkloadItem, 0, len(replicaSets.Items))
		for _, rs := range replicaSets.Items {
			desired := int32(1)
			if rs.Spec.Replicas != nil {
				desired = *rs.Spec.Replicas
			}
			summary.ReplicaSetItems = append(summary.ReplicaSetItems, WorkloadItem{
				Name: rs.Name, Namespace: rs.Namespace, DesiredReplicas: desired, ReadyReplicas: rs.Status.ReadyReplicas,
			})
		}
	}

	if jobs, err := c.clientset.BatchV1().Jobs("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "jobs", err)
	} else {
		summary.Jobs = int32(len(jobs.Items))
		summary.JobItems = make([]JobItem, 0, len(jobs.Items))
		for _, j := range jobs.Items {
			summary.JobItems = append(summary.JobItems, JobItem{
				Name: j.Name, Namespace: j.Namespace, Completions: j.Spec.Completions,
				Succeeded: j.Status.Succeeded, Failed: j.Status.Failed, Active: j.Status.Active,
			})
		}
	}

	if cronJobs, err := c.clientset.BatchV1().CronJobs("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "cronjobs", err)
	} else {
		summary.CronJobs = int32(len(cronJobs.Items))
		summary.CronJobItems = make([]CronJobItem, 0, len(cronJobs.Items))
		for _, cj := range cronJobs.Items {
			item := CronJobItem{
				Name: cj.Name, Namespace: cj.Namespace, Schedule: cj.Spec.Schedule,
				Suspended: cj.Spec.Suspend != nil && *cj.Spec.Suspend, ActiveJobs: int32(len(cj.Status.Active)),
			}
			if cj.Status.LastScheduleTime != nil {
				item.LastScheduleTime = cj.Status.LastScheduleTime.Time.UTC().Format(time.RFC3339)
			}
			summary.CronJobItems = append(summary.CronJobItems, item)
		}
	}

	if services, err := c.clientset.CoreV1().Services("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "services", err)
	} else {
		summary.Services = int32(len(services.Items))
		summary.ServiceItems = make([]ServiceItem, 0, len(services.Items))
		for _, svc := range services.Items {
			ports := make([]string, 0, len(svc.Spec.Ports))
			for _, p := range svc.Spec.Ports {
				ports = append(ports, fmt.Sprintf("%d/%s", p.Port, p.Protocol))
			}
			summary.ServiceItems = append(summary.ServiceItems, ServiceItem{
				Name: svc.Name, Namespace: svc.Namespace, Type: string(svc.Spec.Type), ClusterIP: svc.Spec.ClusterIP, Ports: ports,
			})
		}
	}

	if pvcs, err := c.clientset.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "persistentvolumeclaims", err)
	} else {
		summary.PersistentVolumeClaims = int32(len(pvcs.Items))
		summary.PVCItems = make([]PVCItem, 0, len(pvcs.Items))
		for _, pvc := range pvcs.Items {
			item := PVCItem{Name: pvc.Name, Namespace: pvc.Namespace, Status: string(pvc.Status.Phase)}
			if pvc.Spec.StorageClassName != nil {
				item.StorageClass = *pvc.Spec.StorageClassName
			}
			if cap, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
				bytes := cap.Value()
				item.CapacityBytes = &bytes
			}
			summary.PVCItems = append(summary.PVCItems, item)
		}
	}

	if pvs, err := c.clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "persistentvolumes", err)
	} else {
		summary.PersistentVolumes = int32(len(pvs.Items))
		summary.PVItems = make([]PVItem, 0, len(pvs.Items))
		for _, pv := range pvs.Items {
			item := PVItem{Name: pv.Name, Status: string(pv.Status.Phase), StorageClass: pv.Spec.StorageClassName, ReclaimPolicy: string(pv.Spec.PersistentVolumeReclaimPolicy)}
			if cap, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
				bytes := cap.Value()
				item.CapacityBytes = &bytes
			}
			summary.PVItems = append(summary.PVItems, item)
		}
	}

	if scs, err := c.clientset.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "storageclasses", err)
	} else {
		summary.StorageClasses = int32(len(scs.Items))
		summary.StorageClassItems = make([]StorageClassItem, 0, len(scs.Items))
		for _, sc := range scs.Items {
			reclaim := ""
			if sc.ReclaimPolicy != nil {
				reclaim = string(*sc.ReclaimPolicy)
			}
			isDefault := sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true"
			summary.StorageClassItems = append(summary.StorageClassItems, StorageClassItem{
				Name: sc.Name, Provisioner: sc.Provisioner, ReclaimPolicy: reclaim, IsDefault: isDefault,
			})
		}
	}

	if ingresses, err := c.clientset.NetworkingV1().Ingresses("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "ingresses", err)
	} else {
		summary.Ingresses = int32(len(ingresses.Items))
		summary.IngressItems = make([]IngressItem, 0, len(ingresses.Items))
		for _, ing := range ingresses.Items {
			item := IngressItem{Name: ing.Name, Namespace: ing.Namespace}
			if ing.Spec.IngressClassName != nil {
				item.ClassName = *ing.Spec.IngressClassName
			}
			for _, rule := range ing.Spec.Rules {
				if rule.Host != "" {
					item.Hosts = append(item.Hosts, rule.Host)
				}
			}
			summary.IngressItems = append(summary.IngressItems, item)
		}
	}

	if netpols, err := c.clientset.NetworkingV1().NetworkPolicies("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "networkpolicies", err)
	} else {
		summary.NetworkPolicies = int32(len(netpols.Items))
		summary.NetworkPolicyItems = make([]NetworkPolicyItem, 0, len(netpols.Items))
		for _, np := range netpols.Items {
			types := make([]string, 0, len(np.Spec.PolicyTypes))
			for _, t := range np.Spec.PolicyTypes {
				types = append(types, string(t))
			}
			summary.NetworkPolicyItems = append(summary.NetworkPolicyItems, NetworkPolicyItem{
				Name: np.Name, Namespace: np.Namespace, PolicyTypes: types,
			})
		}
	}

	if slices, err := c.clientset.DiscoveryV1().EndpointSlices("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "endpointslices", err)
	} else {
		summary.EndpointSlices = int32(len(slices.Items))
		summary.EndpointSliceItems = make([]EndpointSliceItem, 0, len(slices.Items))
		for _, es := range slices.Items {
			summary.EndpointSliceItems = append(summary.EndpointSliceItems, EndpointSliceItem{
				Name: es.Name, Namespace: es.Namespace, AddressType: string(es.AddressType), EndpointCount: int32(len(es.Endpoints)),
			})
		}
	}

	if quotas, err := c.clientset.CoreV1().ResourceQuotas("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "resourcequotas", err)
	} else {
		summary.ResourceQuotas = int32(len(quotas.Items))
		summary.ResourceQuotaItems = make([]ResourceQuotaItem, 0, len(quotas.Items))
		for _, rq := range quotas.Items {
			hard := make(map[string]string, len(rq.Spec.Hard))
			for name, qty := range rq.Spec.Hard {
				hard[string(name)] = qty.String()
			}
			summary.ResourceQuotaItems = append(summary.ResourceQuotaItems, ResourceQuotaItem{
				Name: rq.Name, Namespace: rq.Namespace, Hard: hard,
			})
		}
	}

	if limitRanges, err := c.clientset.CoreV1().LimitRanges("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "limitranges", err)
	} else {
		summary.LimitRanges = int32(len(limitRanges.Items))
		summary.LimitRangeItems = make([]LimitRangeItem, 0, len(limitRanges.Items))
		for _, lr := range limitRanges.Items {
			types := make([]string, 0, len(lr.Spec.Limits))
			for _, l := range lr.Spec.Limits {
				types = append(types, string(l.Type))
			}
			summary.LimitRangeItems = append(summary.LimitRangeItems, LimitRangeItem{
				Name: lr.Name, Namespace: lr.Namespace, Types: types,
			})
		}
	}

	if pdbs, err := c.clientset.PolicyV1().PodDisruptionBudgets("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "poddisruptionbudgets", err)
	} else {
		summary.PodDisruptionBudgets = int32(len(pdbs.Items))
		summary.PDBItems = make([]PodDisruptionBudgetItem, 0, len(pdbs.Items))
		for _, pdb := range pdbs.Items {
			item := PodDisruptionBudgetItem{
				Name: pdb.Name, Namespace: pdb.Namespace,
				CurrentHealthy: pdb.Status.CurrentHealthy, DesiredHealthy: pdb.Status.DesiredHealthy, ExpectedPods: pdb.Status.ExpectedPods,
			}
			if pdb.Spec.MinAvailable != nil {
				item.MinAvailable = pdb.Spec.MinAvailable.String()
			}
			if pdb.Spec.MaxUnavailable != nil {
				item.MaxUnavailable = pdb.Spec.MaxUnavailable.String()
			}
			summary.PDBItems = append(summary.PDBItems, item)
		}
	}

	if hpas, err := c.clientset.AutoscalingV2().HorizontalPodAutoscalers("").List(ctx, metav1.ListOptions{}); err != nil {
		logListErr(&summary, "horizontalpodautoscalers", err)
	} else {
		summary.HorizontalPodAutoscalers = int32(len(hpas.Items))
		summary.HPAItems = make([]HPAItem, 0, len(hpas.Items))
		for _, hpa := range hpas.Items {
			item := HPAItem{
				Name: hpa.Name, Namespace: hpa.Namespace, MinReplicas: hpa.Spec.MinReplicas,
				MaxReplicas: hpa.Spec.MaxReplicas, CurrentReplicas: hpa.Status.CurrentReplicas,
			}
			for _, m := range hpa.Spec.Metrics {
				if m.Type == autoscalingv2.ResourceMetricSourceType && m.Resource != nil && m.Resource.Name == corev1.ResourceCPU && m.Resource.Target.AverageUtilization != nil {
					item.TargetCPUPercent = m.Resource.Target.AverageUtilization
					break
				}
			}
			summary.HPAItems = append(summary.HPAItems, item)
		}
	}

	log.Printf("k8s discovery: cluster summary - namespaces=%d nodes=%d pods=%d deployments=%d statefulsets=%d daemonsets=%d replicasets=%d jobs=%d cronjobs=%d services=%d pvcs=%d pvs=%d storageclasses=%d ingresses=%d networkpolicies=%d endpointslices=%d resourcequotas=%d limitranges=%d pdbs=%d hpas=%d",
		summary.Namespaces, summary.Nodes, summary.Pods, summary.Deployments, summary.StatefulSets, summary.DaemonSets, summary.ReplicaSets, summary.Jobs, summary.CronJobs,
		summary.Services, summary.PersistentVolumeClaims, summary.PersistentVolumes, summary.StorageClasses, summary.Ingresses, summary.NetworkPolicies, summary.EndpointSlices,
		summary.ResourceQuotas, summary.LimitRanges, summary.PodDisruptionBudgets, summary.HorizontalPodAutoscalers)
	return summary, nil
}

// StreamLogs answers the "stream_logs" command -- a live, follow tail,
// calling onLine once per line until ctx is cancelled (the backend's
// "stop_stream" command, or the browser disconnecting) or the pod's log
// stream itself ends.
func (c *k8sClient) StreamLogs(ctx context.Context, namespace, podName string, onLine func(string)) error {
	stream, err := c.clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Follow: true, Timestamps: true,
	}).Stream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	// A single log line can exceed bufio.Scanner's 64KiB default (e.g. a
	// large JSON blob printed on one line) -- 1MiB is generous without
	// being unbounded.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		onLine(scanner.Text())
	}
	return scanner.Err()
}
