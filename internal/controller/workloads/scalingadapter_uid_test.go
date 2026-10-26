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

package workloads

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/rbgs/api/workloads/constants"
	workloadsv1alpha2 "sigs.k8s.io/rbgs/api/workloads/v1alpha2"
	"sigs.k8s.io/rbgs/pkg/scale"
)

func TestScalingAdapterReplicasOverride_ForeignAdapterIgnored(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = workloadsv1alpha2.AddToScheme(scheme)

	rbg := &workloadsv1alpha2.RoleBasedGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-rbg-uid"},
		Spec: workloadsv1alpha2.RoleBasedGroupSpec{
			Roles: []workloadsv1alpha2.RoleSpec{{
				Name:           "worker",
				Replicas:       ptr.To[int32](2),
				ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: true},
			}},
		},
	}

	tests := []struct {
		name             string
		adapterOwnerUID  string
		expectedReplicas int32
	}{
		{
			name:             "own adapter overrides replicas",
			adapterOwnerUID:  "new-rbg-uid",
			expectedReplicas: 9,
		},
		{
			name:             "foreign adapter is ignored",
			adapterOwnerUID:  "old-rbg-uid",
			expectedReplicas: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
				ObjectMeta: metav1.ObjectMeta{
					Name:      scale.GenerateScalingAdapterName(rbg.Name, "worker"),
					Namespace: rbg.Namespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "workloads.x-k8s.io/v1alpha2",
						Kind:       "RoleBasedGroup",
						Name:       rbg.Name,
						UID:        types.UID(tt.adapterOwnerUID),
					}},
				},
				Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
					Replicas:       ptr.To[int32](9),
					ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: "worker"},
				},
				Status: workloadsv1alpha2.RoleBasedGroupScalingAdapterStatus{Phase: constants.AdapterPhaseBound},
			}

			testRBG := rbg.DeepCopy()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(adapter).Build()
			r := &RoleBasedGroupReconciler{client: c, apiReader: c, recorder: record.NewFakeRecorder(10)}

			require.NoError(t, r.applyRBGSAReplicasOverride(context.Background(), testRBG))
			assert.Equal(t, tt.expectedReplicas, *testRBG.Spec.Roles[0].Replicas)
		})
	}
}

func TestReconcileScalingAdapter_PredecessorAdapterRecreated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	rbg := &workloadsv1alpha2.RoleBasedGroup{
		TypeMeta:   metav1.TypeMeta{APIVersion: "workloads.x-k8s.io/v1alpha2", Kind: "RoleBasedGroup"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-rbg-uid"},
	}
	enabledRole := &workloadsv1alpha2.RoleSpec{
		Name:           "worker",
		ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: true},
	}
	adapterName := scale.GenerateScalingAdapterName(rbg.Name, enabledRole.Name)

	makeAdapter := func(ownerUID types.UID, replicas *int32) *workloadsv1alpha2.RoleBasedGroupScalingAdapter {
		adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
			ObjectMeta: metav1.ObjectMeta{Name: adapterName, Namespace: rbg.Namespace},
			Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
				Replicas:       replicas,
				ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: enabledRole.Name},
			},
		}
		if ownerUID != "" {
			adapter.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: rbg.APIVersion,
				Kind:       rbg.Kind,
				Name:       rbg.Name,
				UID:        ownerUID,
			}}
		}
		return adapter
	}

	tests := []struct {
		name     string
		existing *workloadsv1alpha2.RoleBasedGroupScalingAdapter
	}{
		{
			name:     "foreign adapter is replaced",
			existing: makeAdapter("old-rbg-uid", ptr.To[int32](9)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.existing).Build()
			r := &RoleBasedGroupReconciler{client: c, apiReader: c}

			require.NoError(t, r.ReconcileScalingAdapter(context.Background(), rbg, enabledRole))

			after := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: adapterName, Namespace: rbg.Namespace}, after))
			assert.True(t, after.ContainsRBGOwner(rbg), "recreated adapter must be owned by the current RBG")
			assert.Nil(t, after.Spec.Replicas, "recreated adapter must not inherit the predecessor's replicas")
			assert.Equal(t, buildScalingAdapterLabels(enabledRole, rbg.Name, enabledRole.Name), after.Labels)
		})
	}
}

func TestReconcileScalingAdapter_DisabledDeletesForeignAdapter(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	rbg := &workloadsv1alpha2.RoleBasedGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-rbg-uid"},
	}
	disabledRole := &workloadsv1alpha2.RoleSpec{Name: "worker"}

	adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scale.GenerateScalingAdapterName(rbg.Name, disabledRole.Name),
			Namespace: rbg.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "workloads.x-k8s.io/v1alpha2",
				Kind:       "RoleBasedGroup",
				Name:       rbg.Name,
				UID:        "old-rbg-uid",
			}},
		},
		Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
			ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: disabledRole.Name},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(adapter).Build()
	r := &RoleBasedGroupReconciler{client: c, apiReader: c}

	require.NoError(t, r.ReconcileScalingAdapter(context.Background(), rbg, disabledRole))

	err := c.Get(context.Background(), client.ObjectKeyFromObject(adapter), &workloadsv1alpha2.RoleBasedGroupScalingAdapter{})
	assert.Error(t, err, "a leftover adapter must be deleted when scaling is disabled, whoever owns it")
}

func TestReconcileScalingAdapter_OwnAdapterDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	rbg := &workloadsv1alpha2.RoleBasedGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-rbg-uid"},
	}
	role := &workloadsv1alpha2.RoleSpec{Name: "worker"}

	adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scale.GenerateScalingAdapterName(rbg.Name, role.Name),
			Namespace: rbg.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "workloads.x-k8s.io/v1alpha2",
				Kind:       "RoleBasedGroup",
				Name:       rbg.Name,
				UID:        rbg.UID,
			}},
		},
		Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
			ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: role.Name},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(adapter).Build()
	r := &RoleBasedGroupReconciler{client: c, apiReader: c}

	err := r.ReconcileScalingAdapter(context.Background(), rbg, role)
	require.NoError(t, err)

	err = c.Get(context.Background(), client.ObjectKeyFromObject(adapter), &workloadsv1alpha2.RoleBasedGroupScalingAdapter{})
	assert.True(t, err != nil && strings.Contains(err.Error(), "not found"), "own adapter with disabled scaling should be deleted")
}

func TestReconcileScalingAdapter_RecreateUsesUIDPrecondition(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	rbg := &workloadsv1alpha2.RoleBasedGroup{
		TypeMeta:   metav1.TypeMeta{APIVersion: "workloads.x-k8s.io/v1alpha2", Kind: "RoleBasedGroup"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-rbg-uid"},
	}
	enabledRole := &workloadsv1alpha2.RoleSpec{
		Name:           "worker",
		ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: true},
	}
	adapterName := scale.GenerateScalingAdapterName(rbg.Name, enabledRole.Name)

	existing := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      adapterName,
			Namespace: rbg.Namespace,
			UID:       "old-adapter-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: rbg.APIVersion,
				Kind:       rbg.Kind,
				Name:       rbg.Name,
				UID:        "old-rbg-uid",
			}},
		},
		Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
			Replicas:       ptr.To[int32](9),
			ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: enabledRole.Name},
		},
	}

	var capturedUID *types.UID
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			var delOpts client.DeleteOptions
			delOpts.ApplyOptions(opts)
			if raw := delOpts.AsDeleteOptions(); raw.Preconditions != nil {
				capturedUID = raw.Preconditions.UID
			}
			return c.Delete(ctx, obj, opts...)
		},
	}).Build()
	r := &RoleBasedGroupReconciler{client: c, apiReader: c}

	require.NoError(t, r.ReconcileScalingAdapter(context.Background(), rbg, enabledRole))

	require.NotNil(t, capturedUID, "delete must include a UID precondition")
	assert.Equal(t, existing.UID, *capturedUID, "delete precondition must match the observed adapter UID")

	after := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: adapterName, Namespace: rbg.Namespace}, after))
	assert.True(t, after.ContainsRBGOwner(rbg), "adapter must be recreated and owned by the current RBG")
}

func TestReconcileScalingAdapter_AdoptsOrphanInPlace(t *testing.T) {
	for _, tt := range []struct {
		name     string
		replicas *int32
		phase    constants.AdapterPhase
		want     int32
	}{
		{"scaled up", ptr.To[int32](5), constants.AdapterPhaseBound, 5},
		{"scaled to zero", ptr.To[int32](0), constants.AdapterPhaseBound, 0},
		{"unset replicas", nil, constants.AdapterPhaseBound, 2},
		{"not bound", ptr.To[int32](5), constants.AdapterPhaseNotBound, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, workloadsv1alpha2.AddToScheme(scheme))
			rbg := &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-parent"},
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{Roles: []workloadsv1alpha2.RoleSpec{{
					Name: "worker", Replicas: ptr.To[int32](2),
					ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: true},
				}}},
			}
			otherOwner := metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "other", UID: "other-uid"}
			adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
				ObjectMeta: metav1.ObjectMeta{
					Name: "demo-worker", Namespace: rbg.Namespace, UID: "retained-adapter",
					OwnerReferences: []metav1.OwnerReference{otherOwner},
					Annotations:     map[string]string{"keep": "value"},
				},
				Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
					Replicas: tt.replicas, ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: "worker"},
				},
				Status: workloadsv1alpha2.RoleBasedGroupScalingAdapterStatus{Phase: tt.phase},
			}
			workload := &workloadsv1alpha2.RoleInstanceSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-worker", Namespace: rbg.Namespace}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rbg, adapter, workload).
				WithStatusSubresource(adapter).WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
					t.Fatal("orphan adoption must not delete the adapter")
					return nil
				},
			}).Build()
			r := &RoleBasedGroupReconciler{client: c, apiReader: c, recorder: record.NewFakeRecorder(10)}
			ctx := context.Background()
			adapterReconciler := &RoleBasedGroupScalingAdapterReconciler{client: c, apiReader: c, scheme: scheme, recorder: record.NewFakeRecorder(10)}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(adapter)}
			result, err := adapterReconciler.Reconcile(ctx, request)
			require.NoError(t, err)
			assert.Equal(t, ctrl.Result{}, result)
			require.NoError(t, r.applyRBGSAReplicasOverride(ctx, rbg))
			assert.Equal(t, tt.want, *rbg.Spec.Roles[0].Replicas)
			require.NoError(t, r.ReconcileScalingAdapter(ctx, rbg, &rbg.Spec.Roles[0]))
			after := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(adapter), after))
			assert.Equal(t, adapter.UID, after.UID)
			assert.Equal(t, adapter.Spec, after.Spec)
			assert.Equal(t, adapter.Status, after.Status)
			assert.Equal(t, adapter.Annotations, after.Annotations)
			assert.Contains(t, after.OwnerReferences, otherOwner)
			assert.True(t, after.ContainsRBGOwner(rbg))
			assert.Equal(t, buildScalingAdapterLabels(&rbg.Spec.Roles[0], rbg.Name, "worker"), after.Labels)
			require.NoError(t, r.ReconcileScalingAdapter(ctx, rbg, &rbg.Spec.Roles[0]))
			unchanged := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(adapter), unchanged))
			assert.Equal(t, after, unchanged)
			require.True(t, RBGScalingAdapterPredicate().Update(event.UpdateEvent{ObjectOld: adapter, ObjectNew: after}))
			if tt.phase == constants.AdapterPhaseNotBound {
				_, err = adapterReconciler.Reconcile(ctx, request)
				require.NoError(t, err)
				bound := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(adapter), bound))
				assert.Equal(t, constants.AdapterPhaseBound, bound.Status.Phase)
			}
		})
	}
}

func TestScalingAdapterReplicasOverride_SurvivesDiscoveryInitialization(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, workloadsv1alpha2.AddToScheme(scheme))
	rbg := &workloadsv1alpha2.RoleBasedGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-parent"},
		Spec: workloadsv1alpha2.RoleBasedGroupSpec{Roles: []workloadsv1alpha2.RoleSpec{{
			Name: "worker", Replicas: ptr.To[int32](2), ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: true},
		}}},
	}
	adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-worker", Namespace: rbg.Namespace},
		Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
			Replicas: ptr.To[int32](5), ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: "worker"},
		},
		Status: workloadsv1alpha2.RoleBasedGroupScalingAdapterStatus{Phase: constants.AdapterPhaseBound},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rbg, adapter).Build()
	r := &RoleBasedGroupReconciler{client: c, apiReader: c, recorder: record.NewFakeRecorder(10)}
	require.NoError(t, r.applyRBGSAReplicasOverride(ctx, rbg))
	requeue, err := r.ensureDiscoveryConfigMode(ctx, rbg)
	require.NoError(t, err)
	assert.False(t, requeue)
	assert.EqualValues(t, 5, *rbg.Spec.Roles[0].Replicas)
	assert.Equal(t, constants.RefineDiscoveryConfigMode, rbg.GetDiscoveryConfigMode())
	persisted := &workloadsv1alpha2.RoleBasedGroup{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(rbg), persisted))
	assert.EqualValues(t, 2, *persisted.Spec.Roles[0].Replicas)
	assert.Equal(t, persisted.ResourceVersion, rbg.ResourceVersion)
	assert.Equal(t, rbg.Annotations, persisted.Annotations)
}

func TestScalingAdapter_RefreshesCachedPredecessorOwner(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, workloadsv1alpha2.AddToScheme(scheme))
	rbg := &workloadsv1alpha2.RoleBasedGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "new-parent"},
		Spec: workloadsv1alpha2.RoleBasedGroupSpec{Roles: []workloadsv1alpha2.RoleSpec{{
			Name: "worker", Replicas: ptr.To[int32](2), ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: true},
		}}},
	}
	orphan := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-worker", Namespace: rbg.Namespace, UID: "retained-adapter"},
		Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
			Replicas: ptr.To[int32](5), ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: "worker"},
		},
		Status: workloadsv1alpha2.RoleBasedGroupScalingAdapterStatus{Phase: constants.AdapterPhaseBound},
	}
	cached := orphan.DeepCopy()
	cached.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "workloads.x-k8s.io/v1alpha1", Kind: "RoleBasedGroup", Name: rbg.Name, UID: "old-parent",
	}}
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rbg, orphan).WithStatusSubresource(orphan).Build()
	c := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if adapter, ok := obj.(*workloadsv1alpha2.RoleBasedGroupScalingAdapter); ok {
				*adapter = *cached.DeepCopy()
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			t.Fatal("stale ownership must not cause a live orphan to be replaced")
			return nil
		},
	})
	r := &RoleBasedGroupReconciler{client: c, apiReader: live, recorder: record.NewFakeRecorder(10)}
	require.NoError(t, r.applyRBGSAReplicasOverride(ctx, rbg))
	assert.EqualValues(t, 5, *rbg.Spec.Roles[0].Replicas)
	require.NoError(t, r.ReconcileScalingAdapter(ctx, rbg, &rbg.Spec.Roles[0]))
	after := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
	require.NoError(t, live.Get(ctx, client.ObjectKeyFromObject(orphan), after))
	assert.Equal(t, orphan.UID, after.UID)
	assert.Equal(t, orphan.Spec, after.Spec)
	assert.Equal(t, orphan.Status, after.Status)
	assert.True(t, after.ContainsRBGOwner(rbg))
}

func TestScalingAdapter_TargetCollisionIsUntouched(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		for _, target := range []*workloadsv1alpha2.AdapterScaleTargetRef{
			{Name: "demo-blue", Role: "worker"},
			{Name: "demo", Role: "another-role"},
			nil,
		} {
			for _, ownerUID := range []types.UID{"", "other-parent", "current-parent"} {
				t.Run(fmt.Sprintf("enabled=%t/target=%v/owner=%s", enabled, target, ownerUID), func(t *testing.T) {
					scheme := runtime.NewScheme()
					require.NoError(t, workloadsv1alpha2.AddToScheme(scheme))
					rbg := &workloadsv1alpha2.RoleBasedGroup{
						ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "current-parent"},
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{Roles: []workloadsv1alpha2.RoleSpec{{
							Name: "blue-worker", Replicas: ptr.To[int32](2),
							ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: enabled},
						}}},
					}
					adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
						ObjectMeta: metav1.ObjectMeta{Name: "demo-blue-worker", Namespace: rbg.Namespace, UID: "existing-adapter"},
						Spec:       workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{Replicas: ptr.To[int32](5), ScaleTargetRef: target},
						Status:     workloadsv1alpha2.RoleBasedGroupScalingAdapterStatus{Phase: constants.AdapterPhaseBound},
					}
					if ownerUID != "" {
						adapter.OwnerReferences = []metav1.OwnerReference{{
							APIVersion: workloadsv1alpha2.GroupVersion.String(), Kind: "RoleBasedGroup", Name: "demo-blue", UID: ownerUID,
						}}
					}
					c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(adapter).Build()
					r := &RoleBasedGroupReconciler{client: c, apiReader: c, recorder: record.NewFakeRecorder(10)}
					ctx := context.Background()
					before := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(adapter), before))
					require.NoError(t, r.applyRBGSAReplicasOverride(ctx, rbg))
					assert.EqualValues(t, 2, *rbg.Spec.Roles[0].Replicas)
					require.ErrorContains(t, r.ReconcileScalingAdapter(ctx, rbg, &rbg.Spec.Roles[0]), "scaleTargetRef does not match")
					after := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(adapter), after))
					assert.Equal(t, before, after)
				})
			}
		}
	}
}

func TestReconcileScalingAdapter_RejectsStaleParentAdoption(t *testing.T) {
	for _, state := range []string{"missing", "replaced", "deleting", "read error"} {
		t.Run(state, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, workloadsv1alpha2.AddToScheme(scheme))
			rbg := &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "cached-parent"},
			}
			role := &workloadsv1alpha2.RoleSpec{Name: "worker", ScalingAdapter: &workloadsv1alpha2.ScalingAdapter{Enable: true}}
			adapter := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{
				ObjectMeta: metav1.ObjectMeta{Name: "demo-worker", Namespace: rbg.Namespace, UID: "orphan"},
				Spec: workloadsv1alpha2.RoleBasedGroupScalingAdapterSpec{
					Replicas: ptr.To[int32](5), ScaleTargetRef: &workloadsv1alpha2.AdapterScaleTargetRef{Name: rbg.Name, Role: role.Name},
				},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rbg, adapter).Build()
			liveBuilder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(adapter)
			live := rbg.DeepCopy()
			switch state {
			case "replaced":
				live.UID = "new-parent"
				liveBuilder.WithObjects(live)
			case "deleting":
				now := metav1.Now()
				live.DeletionTimestamp = &now
				live.Finalizers = []string{"test.example/keep"}
				liveBuilder.WithObjects(live)
			case "read error":
				liveBuilder.WithInterceptorFuncs(interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*workloadsv1alpha2.RoleBasedGroup); ok {
							return fmt.Errorf("API unavailable")
						}
						return c.Get(ctx, key, obj, opts...)
					},
				})
			}
			r := &RoleBasedGroupReconciler{client: c, apiReader: liveBuilder.Build()}
			ctx := context.Background()
			before := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(adapter), before))
			require.ErrorContains(t, r.ReconcileScalingAdapter(ctx, rbg, role), "cannot adopt")
			after := &workloadsv1alpha2.RoleBasedGroupScalingAdapter{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(adapter), after))
			assert.Equal(t, before, after)
		})
	}
}
