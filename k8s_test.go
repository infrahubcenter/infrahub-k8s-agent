package main

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodHealth(t *testing.T) {
	now := time.Now()
	finished := metav1.NewTime(now.Add(-2 * time.Minute))
	clientset := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "web-7d9f8c6b5d-x2k9q", Namespace: "shop"},
			Spec:       corev1.PodSpec{NodeName: "node-1"},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:         "web",
					RestartCount: 4,
					State:        corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off 40s restarting failed container"}},
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason: "OOMKilled", ExitCode: 137, FinishedAt: finished,
					}},
				}},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "api-0", Namespace: "shop"},
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{{
					Name:  "api",
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
				}},
				Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", Message: "0/2 nodes are available"}},
			},
		},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "healthy", Namespace: "shop"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "ev1", Namespace: "shop"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "shop", Name: "api-0"},
			Type:           "Warning", Reason: "FailedMount", Message: "MountVolume.SetUp failed", Count: 3,
			LastTimestamp: metav1.NewTime(now.Add(-time.Minute)),
		},
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "ev-old", Namespace: "shop"},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "shop", Name: "api-0"},
			Type:           "Warning", Reason: "BackOff", Message: "old",
			LastTimestamp: metav1.NewTime(now.Add(-3 * time.Hour)),
		},
	)
	c := &k8sClient{clientset: clientset}

	result, err := c.PodHealth(context.Background(), now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]PodHealth{}
	for _, p := range result.Pods {
		byName[p.PodName] = p
	}

	web := byName["web-7d9f8c6b5d-x2k9q"]
	if web.RestartCount != 4 || len(web.Problems) != 2 {
		t.Fatalf("web: %+v", web)
	}
	if web.Problems[0].Source != "waiting" || web.Problems[0].Reason != "CrashLoopBackOff" {
		t.Errorf("web waiting problem: %+v", web.Problems[0])
	}
	if p := web.Problems[1]; p.Source != "last_terminated" || p.Reason != "OOMKilled" || p.ExitCode == nil || *p.ExitCode != 137 {
		t.Errorf("web last termination: %+v", p)
	}

	api := byName["api-0"]
	reasons := []string{}
	for _, p := range api.Problems {
		reasons = append(reasons, p.Source+":"+p.Reason)
	}
	// ContainerCreating is normal start-up, not a problem; the 3-hour-old
	// event is outside the requested window.
	if len(reasons) != 2 || reasons[0] != "condition:Unschedulable" || reasons[1] != "event:FailedMount" {
		t.Errorf("api problems: %v", reasons)
	}
	if len(byName["healthy"].Problems) != 0 {
		t.Errorf("healthy pod has problems: %+v", byName["healthy"].Problems)
	}
}
