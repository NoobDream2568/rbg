/*
Copyright 2026 The RBG Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha2

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/rbgs/api/workloads/constants"
	workloadsv1alpha2 "sigs.k8s.io/rbgs/api/workloads/v1alpha2"
	"sigs.k8s.io/rbgs/pkg/discovery"
	"sigs.k8s.io/rbgs/test/e2e/framework"
	testutils "sigs.k8s.io/rbgs/test/utils"
	wrappersv2 "sigs.k8s.io/rbgs/test/wrappers/v1alpha2"
	"sigs.k8s.io/yaml"
)

// RunDiscoveryConfigMapTestCases registers e2e tests for the discovery ConfigMap
// generated for stateful roles with mixed patterns.
func RunDiscoveryConfigMapTestCases(f *framework.Framework) {
	ginkgo.Describe("discovery ConfigMap for mixed role patterns", func() {
		ginkgo.It("should expose component-level addresses for multi-pod patterns and keep native behavior for StandalonePattern", func() {
			rbg := buildDiscoveryConfigMapRBG(f.Namespace)

			f.RegisterDebugFn(func() {
				dumpDebugInfo(f, rbg)
				dumpDiscoveryConfigMapDebugInfo(f, rbg)
			})

			gomega.Expect(f.Client.Create(f.Ctx, rbg)).Should(gomega.Succeed())

			// Wait for the RBG to be Ready (router×2 + prefill×2 + decode×2 = 6 pods).
			f.ExpectRbgV2Equal(rbg)

			// Verify the ConfigMap exists and contains correct addresses.
			ginkgo.By("Verifying discovery ConfigMap addresses for all roles")
			verifyDiscoveryConfigMap(f, rbg)
		})
	})
}

// buildDiscoveryConfigMapRBG creates an RBG with three roles:
//   - router  (StandalonePattern):      replicas=2, no components
//   - prefill (LeaderWorkerPattern):    replicas=1, size=2, leader/worker components
//   - decode  (CustomComponentsPattern): replicas=1, leader×1 + worker×1
//
// This produces 6 pods total:
//   - {rbg}-router-{0,1}
//   - {rbg}-prefill-0-{0,1}  (leader/worker)
//   - {rbg}-decode-0-leader-0
//   - {rbg}-decode-0-worker-0
func buildDiscoveryConfigMapRBG(namespace string) *workloadsv1alpha2.RoleBasedGroup {
	leaderTemplate := wrappersv2.BuildBasicPodTemplateSpec()
	leaderTemplate.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent

	workerTemplate := wrappersv2.BuildBasicPodTemplateSpec()
	workerTemplate.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent

	decodeRole := workloadsv1alpha2.RoleSpec{
		Name:     "decode",
		Replicas: ptr.To(int32(1)),
		Pattern: workloadsv1alpha2.Pattern{
			CustomComponentsPattern: &workloadsv1alpha2.CustomComponentsPattern{
				Components: []workloadsv1alpha2.InstanceComponent{
					{
						Name:     "leader",
						Size:     ptr.To(int32(1)),
						Template: leaderTemplate,
					},
					{
						Name:     "worker",
						Size:     ptr.To(int32(1)),
						Template: workerTemplate,
					},
				},
			},
		},
	}

	rbg := wrappersv2.BuildBasicRoleBasedGroup("discovery-cm-test", namespace).
		WithRoles([]workloadsv1alpha2.RoleSpec{
			wrappersv2.BuildStandaloneRole("router").WithReplicas(2).Obj(),
			wrappersv2.BuildLeaderWorkerRole("prefill").WithSize(2).Obj(),
			decodeRole,
		}).Obj()
	return rbg
}

// verifyDiscoveryConfigMap fetches the ConfigMap and verifies:
//   - decode (CustomComponentsPattern) instance exposes component-level addresses
//   - prefill (LeaderWorkerPattern) instance exposes leader/worker pod addresses
//   - router (StandalonePattern) instances keep the native shape without components
func verifyDiscoveryConfigMap(f *framework.Framework, rbg *workloadsv1alpha2.RoleBasedGroup) {
	cm := &corev1.ConfigMap{}
	gomega.Eventually(func() error {
		return f.Client.Get(f.Ctx,
			client.ObjectKey{Name: rbg.Name, Namespace: rbg.Namespace},
			cm,
		)
	}, testutils.Timeout, testutils.Interval).Should(gomega.Succeed(),
		"discovery ConfigMap should exist")

	configData, ok := cm.Data["config.yaml"]
	gomega.Expect(ok).To(gomega.BeTrue(), "ConfigMap should contain config.yaml key")

	var cfg discovery.ClusterConfig
	gomega.Expect(yaml.Unmarshal([]byte(configData), &cfg)).Should(gomega.Succeed(),
		"config.yaml should be valid YAML")

	// Verify the router role (StandalonePattern) keeps the native shape:
	// no component-level addresses, addresses match pod names.
	ginkgo.By("Verifying router (StandalonePattern) instances have no components field")
	routerInfo, ok := cfg.Roles["router"]
	gomega.Expect(ok).To(gomega.BeTrue(), "config should contain 'router' role")
	gomega.Expect(routerInfo.Size).To(gomega.Equal(2), "router role size should be 2")
	gomega.Expect(routerInfo.Instances).To(gomega.HaveLen(2), "router should have 2 instances")
	for i, inst := range routerInfo.Instances {
		gomega.Expect(inst.Components).To(gomega.BeEmpty(),
			fmt.Sprintf("router instance %d should not have component-level addresses", i))
		gomega.Expect(inst.Address).To(gomega.Equal(
			fmt.Sprintf("%s-router-%d.s-%s-router", rbg.Name, i, rbg.Name),
		), fmt.Sprintf("router instance %d address should match pod name", i))
	}

	// Verify the prefill role (LeaderWorkerPattern) exposes the leader and worker
	// pod addresses (RoleInstanceSet naming: {workloadName}-{ordinal}-{podIndex}).
	ginkgo.By("Verifying prefill (LeaderWorkerPattern) component-level addresses")
	prefillInfo, ok := cfg.Roles["prefill"]
	gomega.Expect(ok).To(gomega.BeTrue(), "config should contain 'prefill' role")
	gomega.Expect(prefillInfo.Size).To(gomega.Equal(1), "prefill role size should be 1")
	gomega.Expect(prefillInfo.Instances).To(gomega.HaveLen(1), "prefill should have 1 instance")

	prefillInst := prefillInfo.Instances[0]
	expectedPrefillComponents := map[string]string{
		"leader-0": fmt.Sprintf("%s-prefill-0-0.s-%s-prefill", rbg.Name, rbg.Name),
		"worker-0": fmt.Sprintf("%s-prefill-0-1.s-%s-prefill", rbg.Name, rbg.Name),
	}
	gomega.Expect(prefillInst.Components).To(gomega.Equal(expectedPrefillComponents),
		"prefill instance should expose exactly leader-0 and worker-0 pod addresses")
	gomega.Expect(prefillInst.Address).To(gomega.Equal(
		fmt.Sprintf("%s-prefill-0.s-%s-prefill", rbg.Name, rbg.Name),
	), "prefill instance address should follow the native naming convention")

	// Verify the decode role (CustomComponentsPattern) exposes component-level addresses.
	ginkgo.By("Verifying decode (CustomComponentsPattern) component-level addresses")
	decodeInfo, ok := cfg.Roles["decode"]
	gomega.Expect(ok).To(gomega.BeTrue(), "config should contain 'decode' role")
	gomega.Expect(decodeInfo.Size).To(gomega.Equal(1), "decode role size should be 1")
	gomega.Expect(decodeInfo.Instances).To(gomega.HaveLen(1), "decode should have 1 instance")

	inst := decodeInfo.Instances[0]
	expectedComponents := map[string]string{
		"leader-0": fmt.Sprintf("%s-decode-0-leader-0.s-%s-decode", rbg.Name, rbg.Name),
		"worker-0": fmt.Sprintf("%s-decode-0-worker-0.s-%s-decode", rbg.Name, rbg.Name),
	}
	gomega.Expect(inst.Components).To(gomega.Equal(expectedComponents),
		"decode instance should expose exactly leader-0 and worker-0 component addresses")
	gomega.Expect(inst.Address).To(gomega.Equal(
		fmt.Sprintf("%s-decode-0.s-%s-decode", rbg.Name, rbg.Name),
	), "decode instance address should follow the native naming convention")
}

// dumpDiscoveryConfigMapDebugInfo prints the ConfigMap contents when a test fails.
func dumpDiscoveryConfigMapDebugInfo(f *framework.Framework, rbg *workloadsv1alpha2.RoleBasedGroup) {
	if rbg == nil || !ginkgo.CurrentSpecReport().Failed() {
		return
	}
	fmt.Println("\n========== Discovery ConfigMap Debug Info ==========")

	cm := &corev1.ConfigMap{}
	if err := f.Client.Get(f.Ctx,
		client.ObjectKey{Name: rbg.Name, Namespace: rbg.Namespace},
		cm,
	); err != nil {
		fmt.Printf("[ConfigMap] Failed to get ConfigMap %s: %v\n", rbg.Name, err)
	} else {
		fmt.Printf("[ConfigMap] %s:\n%s\n", rbg.Name, cm.Data["config.yaml"])
	}

	// Dump pods for context.
	podList := &corev1.PodList{}
	if err := f.Client.List(f.Ctx, podList,
		client.InNamespace(rbg.Namespace),
		client.MatchingLabels{constants.GroupNameLabelKey: rbg.Name},
	); err != nil {
		fmt.Printf("[POD] Failed to list pods: %v\n", err)
	} else {
		fmt.Printf("[POD] %d pods:\n", len(podList.Items))
		for _, pod := range podList.Items {
			component := pod.Labels[constants.ComponentNameLabelKey]
			fmt.Printf("  - %s (component=%s) phase=%s hostname=%s subdomain=%s\n",
				pod.Name, component, pod.Status.Phase,
				pod.Spec.Hostname, pod.Spec.Subdomain)
		}
	}

	fmt.Println("========== End Discovery ConfigMap Debug Info ==========")
}
