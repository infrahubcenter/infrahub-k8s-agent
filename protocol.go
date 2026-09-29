// The wire protocol between this agent and InfraHub's backend -- a small,
// stable, JSON-over-WebSocket command/response protocol. This is a
// deliberate, hand-kept-in-sync duplicate of the backend's own copy
// (vmcontrolcenter/backend/internal/services/k8s_client.go): the two are
// genuinely separate deployables (this agent runs inside a user's
// cluster, versioned and upgraded independently of the backend), so a
// shared Go module would only add coupling without much benefit for a
// protocol this small. If you change one side, change the other.
package main

import "encoding/json"

// CommandType enumerates every command the backend can send.
type CommandType string

const (
	CmdServerVersion          CommandType = "server_version"
	CmdListPods               CommandType = "list_pods"
	CmdFetchLogsSince         CommandType = "fetch_logs_since"
	CmdStreamLogs             CommandType = "stream_logs"
	CmdStopStream             CommandType = "stop_stream"
	CmdListNodes              CommandType = "list_nodes"
	CmdClusterResourceSummary CommandType = "cluster_resource_summary"
)

// Command is one backend -> agent message.
type Command struct {
	ID        string      `json:"id"`
	Type      CommandType `json:"type"`
	Namespace string      `json:"namespace,omitempty"`
	PodName   string      `json:"pod_name,omitempty"`
	Since     string      `json:"since,omitempty"`
}

// MessageType enumerates every message this agent can send back.
type MessageType string

const (
	MsgResult  MessageType = "result"
	MsgLogLine MessageType = "log_line"
	MsgDone    MessageType = "done"
	MsgError   MessageType = "error"
)

// Message is one agent -> backend message.
type Message struct {
	ID      string          `json:"id"`
	Type    MessageType     `json:"type"`
	Data    json.RawMessage `json:"data,omitempty"`
	Line    string          `json:"line,omitempty"`
	Message string          `json:"message,omitempty"`
}

// PodInfo is what "list_pods" returns, one entry per pod.
type PodInfo struct {
	Namespace       string `json:"namespace"`
	PodName         string `json:"pod_name"`
	NodeName        string `json:"node_name,omitempty"`
	Phase           string `json:"phase"`
	ReadyContainers int32  `json:"ready_containers"`
	TotalContainers int32  `json:"total_containers"`
	RestartCount    int32  `json:"restart_count"`
	CPUMillicores   *int64 `json:"cpu_millicores,omitempty"`
	MemoryBytes     *int64 `json:"memory_bytes,omitempty"`
	StartedAt       string `json:"started_at,omitempty"`
}

// ServerVersionResult is what "server_version" returns.
type ServerVersionResult struct {
	Version string `json:"version"`
}

// NodeInfo is what "list_nodes" returns, one entry per cluster node.
// Usage fields (CPU/Memory/Storage) are always best-effort: nil when
// metrics-server or the node's kubelet stats API isn't reachable, never a
// fabricated value -- matching PodInfo's own convention.
type NodeInfo struct {
	Name                     string   `json:"name"`
	Ready                    bool     `json:"ready"`
	Roles                    []string `json:"roles,omitempty"`
	KubeletVersion           string   `json:"kubelet_version,omitempty"`
	OSImage                  string   `json:"os_image,omitempty"`
	CPUCapacityMillicores    int64    `json:"cpu_capacity_millicores"`
	CPUAllocatableMillicores int64    `json:"cpu_allocatable_millicores"`
	CPUUsageMillicores       *int64   `json:"cpu_usage_millicores,omitempty"`
	MemoryCapacityBytes      int64    `json:"memory_capacity_bytes"`
	MemoryAllocatableBytes   int64    `json:"memory_allocatable_bytes"`
	MemoryUsageBytes         *int64   `json:"memory_usage_bytes,omitempty"`
	StorageCapacityBytes     *int64   `json:"storage_capacity_bytes,omitempty"`
	StorageUsageBytes        *int64   `json:"storage_usage_bytes,omitempty"`
	PodCapacity              int64    `json:"pod_capacity,omitempty"`
	PodCount                 int32    `json:"pod_count"`
}

// ClusterResourceSummary is what "cluster_resource_summary" returns --
// cluster-wide counts of the resource kinds this agent's RBAC is scoped to
// read, plus (as of the Item lists below) each resource's own identity so
// the backend can offer click-through detail, not just a bare count.
// Deliberately excludes Secrets/ConfigMaps and every RBAC-object kind
// (ServiceAccounts/Roles/RoleBindings/ClusterRoles/ClusterRoleBindings):
// the ClusterRole in deploy/manifest.yaml never grants access to them, so
// a compromised or curious backend can't enumerate a cluster's secret
// material or privilege-escalation surface through this agent, even just
// names. Also deliberately excludes Gateway API kinds and
// VerticalPodAutoscaler -- both are CRD-based (may not be installed on a
// given cluster at all), which needs an existence check this agent
// doesn't do yet.
//
// Every field below is filled in independently by ClusterResourceSummary
// (k8s.go) -- one kind's List() failing (RBAC not yet upgraded to a newer
// agent version, or a kind that genuinely doesn't exist on an older
// cluster) only zeroes that one count/items pair, never the whole
// response. Don't assume a zero count means "none exist"; it may mean
// "couldn't be read" -- check the agent's own logs for which.
type ClusterResourceSummary struct {
	Namespaces               int32 `json:"namespaces"`
	Nodes                    int32 `json:"nodes"`
	Pods                     int32 `json:"pods"`
	Deployments              int32 `json:"deployments"`
	StatefulSets             int32 `json:"stateful_sets"`
	DaemonSets               int32 `json:"daemon_sets"`
	ReplicaSets              int32 `json:"replica_sets"`
	Jobs                     int32 `json:"jobs"`
	CronJobs                 int32 `json:"cron_jobs"`
	Services                 int32 `json:"services"`
	PersistentVolumeClaims   int32 `json:"persistent_volume_claims"`
	PersistentVolumes        int32 `json:"persistent_volumes"`
	StorageClasses           int32 `json:"storage_classes"`
	Ingresses                int32 `json:"ingresses"`
	NetworkPolicies          int32 `json:"network_policies"`
	EndpointSlices           int32 `json:"endpoint_slices"`
	ResourceQuotas           int32 `json:"resource_quotas"`
	LimitRanges              int32 `json:"limit_ranges"`
	PodDisruptionBudgets     int32 `json:"pod_disruption_budgets"`
	HorizontalPodAutoscalers int32 `json:"horizontal_pod_autoscalers"`

	// Item lists mirror each count above one-for-one (len(X Items) ==
	// the matching count field) -- nodes and pods already have their own
	// full-detail commands (list_nodes, list_pods), so they're not
	// duplicated here.
	NamespaceItems     []NamespaceItem           `json:"namespace_items,omitempty"`
	DeploymentItems    []WorkloadItem            `json:"deployment_items,omitempty"`
	StatefulSetItems   []WorkloadItem            `json:"stateful_set_items,omitempty"`
	DaemonSetItems     []WorkloadItem            `json:"daemon_set_items,omitempty"`
	ReplicaSetItems    []WorkloadItem            `json:"replica_set_items,omitempty"`
	JobItems           []JobItem                 `json:"job_items,omitempty"`
	CronJobItems       []CronJobItem             `json:"cron_job_items,omitempty"`
	ServiceItems       []ServiceItem             `json:"service_items,omitempty"`
	PVCItems           []PVCItem                 `json:"pvc_items,omitempty"`
	PVItems            []PVItem                  `json:"pv_items,omitempty"`
	StorageClassItems  []StorageClassItem        `json:"storage_class_items,omitempty"`
	IngressItems       []IngressItem             `json:"ingress_items,omitempty"`
	NetworkPolicyItems []NetworkPolicyItem       `json:"network_policy_items,omitempty"`
	EndpointSliceItems []EndpointSliceItem       `json:"endpoint_slice_items,omitempty"`
	ResourceQuotaItems []ResourceQuotaItem       `json:"resource_quota_items,omitempty"`
	LimitRangeItems    []LimitRangeItem          `json:"limit_range_items,omitempty"`
	PDBItems           []PodDisruptionBudgetItem `json:"pdb_items,omitempty"`
	HPAItems           []HPAItem                 `json:"hpa_items,omitempty"`

	// FailedKinds lists (by the same lowercase keys used in this agent's
	// own log lines, e.g. "jobs", "endpointslices") which kinds' List()
	// call failed this round -- see logListErr. A kind's count/items are
	// always 0/empty when listed here, but the backend must not treat
	// that as "genuinely has none" for removal-marking purposes.
	FailedKinds []string `json:"failed_kinds,omitempty"`
}

// NamespaceItem is one entry in ClusterResourceSummary.NamespaceItems.
type NamespaceItem struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// WorkloadItem is one entry in ClusterResourceSummary's Deployment/
// StatefulSet/DaemonSet item lists -- the three workload kinds share this
// shape (desired vs. ready replica count) even though DaemonSets don't
// have a literal "replicas" spec field (desired/ready-scheduled maps onto
// the same two numbers).
type WorkloadItem struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	DesiredReplicas int32  `json:"desired_replicas"`
	ReadyReplicas   int32  `json:"ready_replicas"`
}

// ServiceItem is one entry in ClusterResourceSummary.ServiceItems.
type ServiceItem struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Type      string   `json:"type"`
	ClusterIP string   `json:"cluster_ip,omitempty"`
	Ports     []string `json:"ports,omitempty"`
}

// PVCItem is one entry in ClusterResourceSummary.PVCItems.
type PVCItem struct {
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	Status        string `json:"status"`
	CapacityBytes *int64 `json:"capacity_bytes,omitempty"`
	StorageClass  string `json:"storage_class,omitempty"`
}

// JobItem is one entry in ClusterResourceSummary.JobItems.
type JobItem struct {
	Name        string `json:"name"`
	Namespace   string `json:"namespace"`
	Completions *int32 `json:"completions,omitempty"`
	Succeeded   int32  `json:"succeeded"`
	Failed      int32  `json:"failed"`
	Active      int32  `json:"active"`
}

// CronJobItem is one entry in ClusterResourceSummary.CronJobItems.
type CronJobItem struct {
	Name             string `json:"name"`
	Namespace        string `json:"namespace"`
	Schedule         string `json:"schedule"`
	Suspended        bool   `json:"suspended"`
	ActiveJobs       int32  `json:"active_jobs"`
	LastScheduleTime string `json:"last_schedule_time,omitempty"`
}

// PVItem is one entry in ClusterResourceSummary.PVItems. PersistentVolumes
// are cluster-scoped (no namespace).
type PVItem struct {
	Name          string `json:"name"`
	Status        string `json:"status"`
	CapacityBytes *int64 `json:"capacity_bytes,omitempty"`
	StorageClass  string `json:"storage_class,omitempty"`
	ReclaimPolicy string `json:"reclaim_policy,omitempty"`
}

// StorageClassItem is one entry in ClusterResourceSummary.StorageClassItems.
// StorageClasses are cluster-scoped (no namespace).
type StorageClassItem struct {
	Name          string `json:"name"`
	Provisioner   string `json:"provisioner"`
	ReclaimPolicy string `json:"reclaim_policy,omitempty"`
	IsDefault     bool   `json:"is_default"`
}

// IngressItem is one entry in ClusterResourceSummary.IngressItems.
type IngressItem struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	ClassName string   `json:"class_name,omitempty"`
	Hosts     []string `json:"hosts,omitempty"`
}

// NetworkPolicyItem is one entry in ClusterResourceSummary.NetworkPolicyItems.
type NetworkPolicyItem struct {
	Name        string   `json:"name"`
	Namespace   string   `json:"namespace"`
	PolicyTypes []string `json:"policy_types,omitempty"`
}

// EndpointSliceItem is one entry in ClusterResourceSummary.EndpointSliceItems.
type EndpointSliceItem struct {
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	AddressType   string `json:"address_type"`
	EndpointCount int32  `json:"endpoint_count"`
}

// ResourceQuotaItem is one entry in ClusterResourceSummary.ResourceQuotaItems
// -- Hard is the configured limit per resource name (e.g. "cpu": "4",
// "memory": "8Gi"), the actual reason this kind is worth showing at all.
type ResourceQuotaItem struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Hard      map[string]string `json:"hard,omitempty"`
}

// LimitRangeItem is one entry in ClusterResourceSummary.LimitRangeItems --
// Types lists which LimitType entries (e.g. "Container", "Pod", "PVC")
// this LimitRange constrains.
type LimitRangeItem struct {
	Name      string   `json:"name"`
	Namespace string   `json:"namespace"`
	Types     []string `json:"types,omitempty"`
}

// PodDisruptionBudgetItem is one entry in ClusterResourceSummary.PDBItems.
type PodDisruptionBudgetItem struct {
	Name           string `json:"name"`
	Namespace      string `json:"namespace"`
	MinAvailable   string `json:"min_available,omitempty"`
	MaxUnavailable string `json:"max_unavailable,omitempty"`
	CurrentHealthy int32  `json:"current_healthy"`
	DesiredHealthy int32  `json:"desired_healthy"`
	ExpectedPods   int32  `json:"expected_pods"`
}

// HPAItem is one entry in ClusterResourceSummary.HPAItems.
type HPAItem struct {
	Name             string `json:"name"`
	Namespace        string `json:"namespace"`
	MinReplicas      *int32 `json:"min_replicas,omitempty"`
	MaxReplicas      int32  `json:"max_replicas"`
	CurrentReplicas  int32  `json:"current_replicas"`
	TargetCPUPercent *int32 `json:"target_cpu_percent,omitempty"`
}
