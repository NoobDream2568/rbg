/*
Copyright 2025 The RBG Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/rbgs/api/workloads/constants"
	workloadsv1alpha2 "sigs.k8s.io/rbgs/api/workloads/v1alpha2"
)

// TestRoleBasedGroupSetReconciler_scaleUp tests the scaleUp function.
func TestRoleBasedGroupSetReconciler_scaleUp(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	// Create a RoleBasedGroupSet for testing
	rbgset := &workloadsv1alpha2.RoleBasedGroupSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rbgset",
			Namespace: "default",
			UID:       "test-uid",
		},
		Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
			GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{
						{Name: "role-1"},
						{Name: "role-2"},
					},
				},
			},
		},
	}

	tests := []struct {
		name        string
		count       int
		expectError bool
	}{
		{
			name:        "Create 3 RBGs",
			count:       3,
			expectError: false,
		},
		{
			name:        "Create 0 RBGs",
			count:       0,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				// Create test reconciler with a fake client
				r := &RoleBasedGroupSetReconciler{
					client: fake.NewClientBuilder().WithScheme(scheme).Build(),
					scheme: scheme,
				}

				// The new scaleUp function expects a list of objects to create.
				// We generate this list based on the test case count.
				var rbgsToCreate []*workloadsv1alpha2.RoleBasedGroup
				for i := 0; i < tt.count; i++ {
					rbgsToCreate = append(rbgsToCreate, newRBGForSet(rbgset, i))
				}

				err := r.scaleUp(context.Background(), rbgset, rbgsToCreate)
				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}

				// Verify the result by listing the created objects.
				var rbglist workloadsv1alpha2.RoleBasedGroupList
				opts := []client.ListOption{
					client.InNamespace(rbgset.Namespace),
					client.MatchingLabels{constants.GroupSetNameLabelKey: rbgset.Name},
				}
				err = r.client.List(context.Background(), &rbglist, opts...)
				assert.NoError(t, err)
				assert.Equal(t, tt.count, len(rbglist.Items))
			},
		)
	}
}

// TestRoleBasedGroupSetReconciler_scaleDown tests the scaleDown function.
func TestRoleBasedGroupSetReconciler_scaleDown(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	rbgBase := []workloadsv1alpha2.RoleBasedGroup{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rbg-0",
				Namespace: "default",
				Labels: map[string]string{
					constants.GroupSetNameLabelKey:  "rbgs-test",
					constants.GroupSetIndexLabelKey: "0",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rbg-1",
				Namespace: "default",
				Labels: map[string]string{
					constants.GroupSetNameLabelKey:  "rbgs-test",
					constants.GroupSetIndexLabelKey: "1",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rbg-2",
				Namespace: "default",
				Labels: map[string]string{
					constants.GroupSetNameLabelKey:  "rbgs-test",
					constants.GroupSetIndexLabelKey: "2",
				},
			},
		},
	}

	tests := []struct {
		name                string
		initialRBGs         []workloadsv1alpha2.RoleBasedGroup
		rbgsToDeleteIndices []int // Indices from initialRBGs to delete
		expectedNamesLeft   []string
	}{
		{
			name:                "Delete 2 out of 3 RBGs",
			initialRBGs:         rbgBase,
			rbgsToDeleteIndices: []int{0, 2}, // Delete rbg-1 and rbg-3
			expectedNamesLeft:   []string{"rbg-1"},
		},
		{
			name:                "Delete all RBGs",
			initialRBGs:         rbgBase,
			rbgsToDeleteIndices: []int{0, 1, 2},
			expectedNamesLeft:   []string{},
		},
		{
			name:                "Delete 0 items",
			initialRBGs:         rbgBase,
			rbgsToDeleteIndices: []int{},
			expectedNamesLeft:   []string{"rbg-0", "rbg-1", "rbg-2"},
		},
		{
			name:                "Delete from an empty list",
			initialRBGs:         []workloadsv1alpha2.RoleBasedGroup{},
			rbgsToDeleteIndices: []int{},
			expectedNamesLeft:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				// Prepare initial objects for the fake client
				objs := make([]runtime.Object, len(tt.initialRBGs))
				for i := range tt.initialRBGs {
					objs[i] = &tt.initialRBGs[i]
				}
				r := &RoleBasedGroupSetReconciler{
					client: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build(),
				}

				// The new scaleDown function expects an explicit list of objects to delete.
				var rbgsToDelete []*workloadsv1alpha2.RoleBasedGroup
				for _, index := range tt.rbgsToDeleteIndices {
					// We need to pass pointers to copies to avoid issues with loop variables.
					rbgCopy := tt.initialRBGs[index].DeepCopy()
					rbgsToDelete = append(rbgsToDelete, rbgCopy)
				}

				err := r.scaleDown(context.Background(), rbgsToDelete)
				assert.NoError(t, err)

				// Verify the result by listing the remaining objects.
				var leftRbgList workloadsv1alpha2.RoleBasedGroupList
				opts := []client.ListOption{
					client.InNamespace("default"),
					client.MatchingLabels{constants.GroupSetNameLabelKey: "rbgs-test"},
				}
				err = r.client.List(context.Background(), &leftRbgList, opts...)
				assert.NoError(t, err)
				assert.Equal(t, len(tt.expectedNamesLeft), len(leftRbgList.Items))

				// Check if the correct items are left.
				remainingNames := make(map[string]bool)
				for _, rbg := range leftRbgList.Items {
					remainingNames[rbg.Name] = true
				}
				for _, expectedName := range tt.expectedNamesLeft {
					assert.True(
						t, remainingNames[expectedName],
						fmt.Sprintf("Expected RBG %s to remain, but it was deleted", expectedName),
					)
				}
			},
		)
	}
}

// TestRoleBasedGroupSetReconciler_needsUpdate tests the needsUpdate method.
func TestRoleBasedGroupSetReconciler_needsUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	tests := []struct {
		name           string
		rbgset         *workloadsv1alpha2.RoleBasedGroupSet
		rbg            *workloadsv1alpha2.RoleBasedGroup
		expectedUpdate bool
	}{
		{
			name: "RBG needs update - different roles",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rbgset",
					Namespace: "default",
				},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{
								{Name: "new-role"},
							},
						},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rbgset",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{
						{Name: "old-role"},
					},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "RBG needs update - template annotation added",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Annotations: map[string]string{
							"app.io/env": "prod",
						},
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "RBG needs update - template annotation removed",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"app.io/env": "prod"},
				},
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "RBG needs update - template label added",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels: map[string]string{"tier": "backend"},
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
					},
				},
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "RBG needs update - template label removed",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
						"tier":                          "backend", // extra label not in template
					},
				},
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "RBG no update needed - roles and metadata all match",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels:      map[string]string{"tier": "backend"},
						Annotations: map[string]string{"app.io/env": "prod"},
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
						"tier":                          "backend",
					},
					Annotations: map[string]string{"app.io/env": "prod"},
				},
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
				},
			},
			expectedUpdate: false,
		},
		{
			name: "RBG no update needed - empty template metadata",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
				},
			},
			expectedUpdate: false,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				r := &RoleBasedGroupSetReconciler{}
				result := r.needsUpdate(tt.rbgset, tt.rbg)
				assert.Equal(t, tt.expectedUpdate, result)
			},
		)
	}
}

// TestRoleBasedGroupSetReconciler_needsTemplateAnnotationUpdate tests the needsTemplateAnnotationUpdate method.
func TestRoleBasedGroupSetReconciler_needsTemplateAnnotationUpdate(t *testing.T) {
	tests := []struct {
		name           string
		rbgset         *workloadsv1alpha2.RoleBasedGroupSet
		rbg            *workloadsv1alpha2.RoleBasedGroup
		expectedUpdate bool
	}{
		{
			name: "RBG has annotation, template doesn't",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"app.io/env": "prod"},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "Template has annotation, RBG doesn't",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Annotations: map[string]string{"app.io/env": "prod"},
					},
				},
			},
			rbg:            &workloadsv1alpha2.RoleBasedGroup{},
			expectedUpdate: true,
		},
		{
			name: "Different annotation values",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Annotations: map[string]string{"app.io/env": "prod"},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"app.io/env": "staging"},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "Same annotation values",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Annotations: map[string]string{"app.io/env": "prod"},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"app.io/env": "prod"},
				},
			},
			expectedUpdate: false,
		},
		{
			name: "Both empty",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{},
				},
			},
			rbg:            &workloadsv1alpha2.RoleBasedGroup{},
			expectedUpdate: false,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				r := &RoleBasedGroupSetReconciler{}
				result := r.needsTemplateAnnotationUpdate(tt.rbgset, tt.rbg)
				assert.Equal(t, tt.expectedUpdate, result)
			},
		)
	}
}

// TestRoleBasedGroupSetReconciler_needsTemplateLabelUpdate tests the needsTemplateLabelUpdate method.
func TestRoleBasedGroupSetReconciler_needsTemplateLabelUpdate(t *testing.T) {
	tests := []struct {
		name           string
		rbgset         *workloadsv1alpha2.RoleBasedGroupSet
		rbg            *workloadsv1alpha2.RoleBasedGroup
		expectedUpdate bool
	}{
		{
			name: "RBG has extra non-system label not in template",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
						"tier":                          "backend",
					},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "Template has label, RBG doesn't",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels: map[string]string{"tier": "backend"},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
					},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "Different label values",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels: map[string]string{"tier": "frontend"},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
						"tier":                          "backend",
					},
				},
			},
			expectedUpdate: true,
		},
		{
			name: "Labels match, system labels ignored in comparison",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels: map[string]string{"tier": "backend"},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
						"tier":                          "backend",
					},
				},
			},
			expectedUpdate: false,
		},
		{
			name: "System labels only, no template labels",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
					},
				},
			},
			expectedUpdate: false,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				r := &RoleBasedGroupSetReconciler{}
				result := r.needsTemplateLabelUpdate(tt.rbgset, tt.rbg)
				assert.Equal(t, tt.expectedUpdate, result)
			},
		)
	}
}

// TestNewRBGForSet_MetadataPropagation tests that newRBGForSet correctly propagates
// groupTemplate.labels and groupTemplate.annotations to the child RBG.
func TestNewRBGForSet_MetadataPropagation(t *testing.T) {
	tests := []struct {
		name                string
		rbgset              *workloadsv1alpha2.RoleBasedGroupSet
		index               int
		expectedLabels      map[string]string
		expectedAnnotations map[string]string
	}{
		{
			name: "Template labels and annotations are propagated",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset", Namespace: "default"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels:      map[string]string{"tier": "backend", "env": "prod"},
						Annotations: map[string]string{"app.io/config": "v1"},
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			index: 2,
			expectedLabels: map[string]string{
				constants.GroupSetNameLabelKey:  "test-rbgset",
				constants.GroupSetIndexLabelKey: "2",
				"tier":                          "backend",
				"env":                           "prod",
			},
			expectedAnnotations: map[string]string{"app.io/config": "v1"},
		},
		{
			name: "Empty template metadata produces only system labels and no annotations",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset", Namespace: "default"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			index: 0,
			expectedLabels: map[string]string{
				constants.GroupSetNameLabelKey:  "test-rbgset",
				constants.GroupSetIndexLabelKey: "0",
			},
			expectedAnnotations: nil,
		},
		{
			name: "Template label does not override system-managed labels",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset", Namespace: "default"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						// Attempting to override a system label via template is ignored
						// because system labels are written after template labels.
						Labels: map[string]string{
							constants.GroupSetIndexLabelKey: "99",
							"tier":                          "backend",
						},
						Spec: workloadsv1alpha2.RoleBasedGroupSpec{
							Roles: []workloadsv1alpha2.RoleSpec{{Name: "role-1"}},
						},
					},
				},
			},
			index: 1,
			expectedLabels: map[string]string{
				constants.GroupSetNameLabelKey:  "test-rbgset",
				constants.GroupSetIndexLabelKey: "1", // system label wins, index is 1 not 99
				"tier":                          "backend",
			},
			expectedAnnotations: nil,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				rbg := newRBGForSet(tt.rbgset, tt.index)
				assert.Equal(t, tt.expectedLabels, rbg.Labels)
				assert.Equal(t, tt.expectedAnnotations, rbg.Annotations)
				assert.Equal(
					t,
					fmt.Sprintf("%s-%d", tt.rbgset.Name, tt.index),
					rbg.Name,
				)
				assert.Equal(t, tt.rbgset.Namespace, rbg.Namespace)
			},
		)
	}
}

// TestSyncRBGMetadata tests the syncRBGMetadata method.
func TestSyncRBGMetadata(t *testing.T) {
	tests := []struct {
		name                string
		rbgset              *workloadsv1alpha2.RoleBasedGroupSet
		rbg                 *workloadsv1alpha2.RoleBasedGroup
		expectedLabels      map[string]string
		expectedAnnotations map[string]string
	}{
		{
			name: "Syncs template labels and annotations, preserves system labels",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels:      map[string]string{"tier": "backend"},
						Annotations: map[string]string{"app.io/env": "prod"},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "1",
						"old-label":                     "old-value",
					},
					Annotations: map[string]string{"old-annotation": "old-value"},
				},
			},
			expectedLabels: map[string]string{
				constants.GroupSetNameLabelKey:  "test-rbgset",
				constants.GroupSetIndexLabelKey: "1",
				"tier":                          "backend",
			},
			expectedAnnotations: map[string]string{"app.io/env": "prod"},
		},
		{
			name: "Clears annotations when template has none",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
					},
					Annotations: map[string]string{"stale-annotation": "value"},
				},
			},
			expectedLabels: map[string]string{
				constants.GroupSetNameLabelKey:  "test-rbgset",
				constants.GroupSetIndexLabelKey: "0",
			},
			expectedAnnotations: nil,
		},
		{
			name: "Removes extra non-system labels not in template",
			rbgset: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset"},
				Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
					GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
						Labels: map[string]string{"env": "prod"},
					},
				},
			},
			rbg: &workloadsv1alpha2.RoleBasedGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						constants.GroupSetNameLabelKey:  "test-rbgset",
						constants.GroupSetIndexLabelKey: "0",
						"stale-label":                   "old",
					},
				},
			},
			expectedLabels: map[string]string{
				constants.GroupSetNameLabelKey:  "test-rbgset",
				constants.GroupSetIndexLabelKey: "0",
				"env":                           "prod",
			},
			expectedAnnotations: nil,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				r := &RoleBasedGroupSetReconciler{}
				r.syncRBGMetadata(tt.rbgset, tt.rbg)
				assert.Equal(t, tt.expectedLabels, tt.rbg.Labels)
				assert.Equal(t, tt.expectedAnnotations, tt.rbg.Annotations)
			},
		)
	}
}

// TestRoleBasedGroupSetReconciler_Reconcile_OptimizedOrder tests the optimized operation order.
// This test verifies that when both role changes and replica reduction occur,
// the controller deletes excess RBGs first, then updates remaining ones.
func TestRoleBasedGroupSetReconciler_Reconcile_OptimizedOrder(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	// Setup: 4 RBGs with old role, scale down to 2 with new role and updated metadata
	initialRBGSet := &workloadsv1alpha2.RoleBasedGroupSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rbgset",
			Namespace: "default",
		},
		Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
			Replicas: ptr.To(int32(2)), // Reduced from 4 to 2
			GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
				Labels:      map[string]string{"env": "prod"},
				Annotations: map[string]string{constants.GroupExclusiveTopologyKey: "new-exclusive"},
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{
					Roles: []workloadsv1alpha2.RoleSpec{{Name: "new-role"}},
				},
			},
		},
	}

	// Create 4 existing RBGs with old role and stale metadata
	existingRBGs := []runtime.Object{initialRBGSet}
	for i := 0; i < 4; i++ {
		rbg := &workloadsv1alpha2.RoleBasedGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-rbgset-%d", i),
				Namespace: "default",
				Labels: map[string]string{
					constants.GroupSetNameLabelKey:  "test-rbgset",
					constants.GroupSetIndexLabelKey: fmt.Sprintf("%d", i),
				},
				Annotations: map[string]string{
					constants.GroupExclusiveTopologyKey: "old-exclusive",
				},
			},
			Spec: workloadsv1alpha2.RoleBasedGroupSpec{
				Roles: []workloadsv1alpha2.RoleSpec{{Name: "old-role"}},
			},
		}
		existingRBGs = append(existingRBGs, rbg)
	}

	r := &RoleBasedGroupSetReconciler{
		client: fake.NewClientBuilder().WithScheme(scheme).
			WithRuntimeObjects(existingRBGs...).
			WithStatusSubresource(&workloadsv1alpha2.RoleBasedGroupSet{}).Build(),
		scheme: scheme,
	}

	// Run reconcile
	_, err := r.Reconcile(
		context.TODO(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: "default",
				Name:      "test-rbgset",
			},
		},
	)
	assert.NoError(t, err)

	// Verify results: should have exactly 2 RBGs
	var rbgList workloadsv1alpha2.RoleBasedGroupList
	err = r.client.List(
		context.Background(), &rbgList,
		client.InNamespace("default"),
		client.MatchingLabels{constants.GroupSetNameLabelKey: "test-rbgset"},
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rbgList.Items))

	// Verify remaining RBGs have updated roles, labels, and annotations from groupTemplate
	for _, rbg := range rbgList.Items {
		assert.Equal(t, "new-role", rbg.Spec.Roles[0].Name)
		// Template annotation should be propagated
		assert.Equal(t, "new-exclusive", rbg.Annotations[constants.GroupExclusiveTopologyKey])
		// Template label should be propagated
		assert.Equal(t, "prod", rbg.Labels["env"])
		// System labels must be preserved
		assert.Equal(t, "test-rbgset", rbg.Labels[constants.GroupSetNameLabelKey])
		index := rbg.Labels[constants.GroupSetIndexLabelKey]
		assert.True(t, index == "0" || index == "1")
	}
}

// TestRoleBasedGroupSetReconciler_Reconcile_StatusUpdate tests the status update logic within the Reconcile loop.
func TestRoleBasedGroupSetReconciler_Reconcile_StatusUpdate(t *testing.T) {
	// Setup test scheme
	scheme := runtime.NewScheme()
	_ = workloadsv1alpha2.AddToScheme(scheme)

	tests := []struct {
		name                string
		initialRBGSet       *workloadsv1alpha2.RoleBasedGroupSet
		rbgList             []workloadsv1alpha2.RoleBasedGroup
		expectReady         bool
		expectReplicas      int32
		expectReadyReplicas int32
		expectedReason      string
		expectedMessagePart string
	}{
		{
			name: "All RBGs ready, replicas match spec",
			initialRBGSet: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset", Namespace: "default"},
				Spec:       workloadsv1alpha2.RoleBasedGroupSetSpec{Replicas: ptr.To(int32(2))},
			},
			rbgList: []workloadsv1alpha2.RoleBasedGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rbg-0",
						Namespace: "default",
						Labels: map[string]string{
							constants.GroupSetNameLabelKey:  "test-rbgset",
							constants.GroupSetIndexLabelKey: "0",
						},
					},
					Status: workloadsv1alpha2.RoleBasedGroupStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(workloadsv1alpha2.RoleBasedGroupReady),
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rbg-1",
						Namespace: "default",
						Labels: map[string]string{
							constants.GroupSetNameLabelKey:  "test-rbgset",
							constants.GroupSetIndexLabelKey: "1",
						},
					},
					Status: workloadsv1alpha2.RoleBasedGroupStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(workloadsv1alpha2.RoleBasedGroupReady),
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
			expectReady:         true,
			expectReplicas:      2,
			expectReadyReplicas: 2,
			expectedReason:      "AllReplicasReady",
			expectedMessagePart: "All RoleBasedGroup replicas are ready.",
		},
		{
			name: "Partial RBGs ready",
			initialRBGSet: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset", Namespace: "default"},
				Spec:       workloadsv1alpha2.RoleBasedGroupSetSpec{Replicas: ptr.To(int32(2))},
			},
			rbgList: []workloadsv1alpha2.RoleBasedGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rbg-0",
						Namespace: "default",
						Labels: map[string]string{
							constants.GroupSetNameLabelKey:  "test-rbgset",
							constants.GroupSetIndexLabelKey: "0",
						},
					},
					Status: workloadsv1alpha2.RoleBasedGroupStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(workloadsv1alpha2.RoleBasedGroupReady),
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rbg-1",
						Namespace: "default",
						Labels: map[string]string{
							constants.GroupSetNameLabelKey:  "test-rbgset",
							constants.GroupSetIndexLabelKey: "1",
						},
					},
					Status: workloadsv1alpha2.RoleBasedGroupStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(workloadsv1alpha2.RoleBasedGroupReady),
								Status: metav1.ConditionFalse,
							},
						},
					},
				},
			},
			expectReady:         false,
			expectReplicas:      2,
			expectReadyReplicas: 1,
			expectedReason:      "ReplicasNotReady",
			expectedMessagePart: "Waiting for replicas to be ready (1/2)",
		},
		{
			name: "No RBGs ready",
			initialRBGSet: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset", Namespace: "default"},
				Spec:       workloadsv1alpha2.RoleBasedGroupSetSpec{Replicas: ptr.To(int32(1))},
			},
			rbgList: []workloadsv1alpha2.RoleBasedGroup{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "rbg-0",
						Namespace: "default",
						Labels: map[string]string{
							constants.GroupSetNameLabelKey:  "test-rbgset",
							constants.GroupSetIndexLabelKey: "0",
						},
					},
					Status: workloadsv1alpha2.RoleBasedGroupStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(workloadsv1alpha2.RoleBasedGroupReady),
								Status: metav1.ConditionFalse,
							},
						},
					},
				},
			},
			expectReady:         false,
			expectReplicas:      1,
			expectReadyReplicas: 0,
			expectedReason:      "ReplicasNotReady",
			expectedMessagePart: "Waiting for replicas to be ready (0/1)",
		},
		{
			name: "Empty RBG list with zero replicas spec",
			initialRBGSet: &workloadsv1alpha2.RoleBasedGroupSet{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rbgset", Namespace: "default"},
				Spec:       workloadsv1alpha2.RoleBasedGroupSetSpec{Replicas: ptr.To(int32(0))},
			},
			rbgList:             []workloadsv1alpha2.RoleBasedGroup{},
			expectReady:         true, // 0 ready >= 0 desired, so it's considered ready.
			expectReplicas:      0,
			expectReadyReplicas: 0,
			expectedReason:      "AllReplicasReady",
			expectedMessagePart: "All RoleBasedGroup replicas are ready.",
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				// Prepare all objects for the fake client.
				objs := []runtime.Object{tt.initialRBGSet}
				for i := range tt.rbgList {
					objs = append(objs, &tt.rbgList[i])
				}

				// Configure the fake client to provide a status subresource for the CRD.
				r := &RoleBasedGroupSetReconciler{
					client: fake.NewClientBuilder().WithScheme(scheme).
						WithRuntimeObjects(objs...).
						WithStatusSubresource(&workloadsv1alpha2.RoleBasedGroupSet{}).Build(),
					scheme: scheme,
				}

				// Run the full reconcile loop. Since replicas in spec match the number of existing
				// objects, no scaling will occur, and it will proceed to status update.
				_, err := r.Reconcile(
					context.TODO(), ctrl.Request{
						NamespacedName: types.NamespacedName{
							Namespace: tt.initialRBGSet.Namespace,
							Name:      tt.initialRBGSet.Name,
						},
					},
				)
				// We expect a RequeueAfter, so we don't assert a nil error, just no *real* error.
				assert.True(t, err == nil || err.Error() == "", "Reconcile returned an unexpected error: %v", err)

				// Fetch the updated RBGSet to check its status.
				updatedRBGSet := &workloadsv1alpha2.RoleBasedGroupSet{}
				err = r.client.Get(
					context.Background(), types.NamespacedName{
						Name:      tt.initialRBGSet.Name,
						Namespace: tt.initialRBGSet.Namespace,
					}, updatedRBGSet,
				)
				assert.NoError(t, err)

				// Verify status fields
				assert.Equal(t, tt.expectReplicas, updatedRBGSet.Status.Replicas)
				assert.Equal(t, tt.expectReadyReplicas, updatedRBGSet.Status.ReadyReplicas)

				// Verify condition
				assert.NotEmpty(t, updatedRBGSet.Status.Conditions, "Status conditions should not be empty")
				condition := updatedRBGSet.Status.Conditions[0]
				assert.Equal(t, string(workloadsv1alpha2.RoleBasedGroupSetReady), condition.Type)
				assert.Equal(t, tt.expectedReason, condition.Reason)
				assert.Contains(t, condition.Message, tt.expectedMessagePart)

				if tt.expectReady {
					assert.Equal(t, metav1.ConditionTrue, condition.Status)
				} else {
					assert.Equal(t, metav1.ConditionFalse, condition.Status)
				}
			},
		)
	}
}

// --- Rolling update tests ---

func rollingTestSet(
	name string, replicas int32, roles []workloadsv1alpha2.RoleSpec, ru *workloadsv1alpha2.GroupSetRolloutStrategy,
) *workloadsv1alpha2.RoleBasedGroupSet {
	return &workloadsv1alpha2.RoleBasedGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{
			Replicas: ptr.To(replicas),
			GroupTemplate: workloadsv1alpha2.RoleBasedGroupTemplateSpec{
				Spec: workloadsv1alpha2.RoleBasedGroupSpec{Roles: roles},
			},
			RolloutStrategy: ru,
		},
	}
}

func rollingTestChild(
	setName string, ordinal int, roles []workloadsv1alpha2.RoleSpec, ready bool,
) *workloadsv1alpha2.RoleBasedGroup {
	readyStatus := metav1.ConditionFalse
	if ready {
		readyStatus = metav1.ConditionTrue
	}
	return &workloadsv1alpha2.RoleBasedGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", setName, ordinal),
			Namespace: "default",
			Labels: map[string]string{
				constants.GroupSetNameLabelKey:  setName,
				constants.GroupSetIndexLabelKey: fmt.Sprintf("%d", ordinal),
			},
		},
		Spec: workloadsv1alpha2.RoleBasedGroupSpec{Roles: roles},
		Status: workloadsv1alpha2.RoleBasedGroupStatus{
			Conditions: []metav1.Condition{
				{
					Type:               string(workloadsv1alpha2.RoleBasedGroupReady),
					Status:             readyStatus,
					Reason:             "Test",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
}

func newRollingTestReconciler(scheme *runtime.Scheme, objs ...runtime.Object) *RoleBasedGroupSetReconciler {
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(objs...).
		WithStatusSubresource(&workloadsv1alpha2.RoleBasedGroupSet{}).Build()
	return &RoleBasedGroupSetReconciler{
		client: c,
		// Reconcile never reads through apiReader; only CheckCrdExists does, at startup.
		scheme:   scheme,
		recorder: record.NewFakeRecorder(100),
	}
}

func reconcileSet(t *testing.T, r *RoleBasedGroupSetReconciler, name string) {
	t.Helper()
	_, err := r.Reconcile(
		context.TODO(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}},
	)
	assert.NoError(t, err)
}

func listChildren(t *testing.T, r *RoleBasedGroupSetReconciler, setName string) []workloadsv1alpha2.RoleBasedGroup {
	t.Helper()
	var list workloadsv1alpha2.RoleBasedGroupList
	err := r.client.List(
		context.Background(), &list,
		client.InNamespace("default"),
		client.MatchingLabels{constants.GroupSetNameLabelKey: setName},
	)
	assert.NoError(t, err)
	return list.Items
}

func getChild(t *testing.T, r *RoleBasedGroupSetReconciler, name string) *workloadsv1alpha2.RoleBasedGroup {
	t.Helper()
	rbg := &workloadsv1alpha2.RoleBasedGroup{}
	err := r.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, rbg)
	assert.NoError(t, err)
	return rbg
}

func markChildReady(t *testing.T, r *RoleBasedGroupSetReconciler, name string) {
	t.Helper()
	rbg := getChild(t, r, name)
	rbg.Status.Conditions = []metav1.Condition{
		{
			Type:               string(workloadsv1alpha2.RoleBasedGroupReady),
			Status:             metav1.ConditionTrue,
			Reason:             "TestReady",
			LastTransitionTime: metav1.Now(),
		},
	}
	assert.NoError(t, r.client.Update(context.Background(), rbg))
}

// getSet re-reads the RoleBasedGroupSet so a test sees the status the reconcile just wrote.
func getSet(t *testing.T, r *RoleBasedGroupSetReconciler, name string) *workloadsv1alpha2.RoleBasedGroupSet {
	t.Helper()
	set := &workloadsv1alpha2.RoleBasedGroupSet{}
	assert.NoError(t, r.client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, set))
	return set
}

func rbgsTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	assert.NoError(t, workloadsv1alpha2.AddToScheme(scheme))
	return scheme
}

// TestRollingUpdate_PacedRecreate verifies the recreate rolling update: one outdated group is
// deleted per budget slot, recreated from the new template, and the rollout waits for it to
// become ready before touching the next one.
func TestRollingUpdate_PacedRecreate(t *testing.T) {
	scheme := rbgsTestScheme(t)

	oldRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	newRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1)), MinReadySeconds: 10}}

	set := rollingTestSet("s", 3, newRoles, &workloadsv1alpha2.GroupSetRolloutStrategy{})
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, oldRoles, true),
		rollingTestChild("s", 1, oldRoles, true),
		rollingTestChild("s", 2, oldRoles, true),
	)

	// First pass deletes only the highest-ordinal outdated group (maxUnavailable defaults to 1).
	reconcileSet(t, r, "s")
	assert.Len(t, listChildren(t, r, "s"), 2)

	// Second pass recreates it from the new template, but it is not ready yet, so the
	// budget stays exhausted and no further group is deleted.
	reconcileSet(t, r, "s")
	children := listChildren(t, r, "s")
	assert.Len(t, children, 3)
	assert.Equal(t, newRoles, getChild(t, r, "s-2").Spec.Roles)

	// Once the recreated group turns ready, the next outdated group is deleted.
	markChildReady(t, r, "s-2")
	reconcileSet(t, r, "s")
	children = listChildren(t, r, "s")
	assert.Len(t, children, 2)
	names := map[string]bool{children[0].Name: true, children[1].Name: true}
	assert.True(t, names["s-0"] && names["s-2"], "expected s-1 to be deleted, got %v", names)
}

// TestRecreateDeleteCarriesUIDPrecondition verifies that a recreate delete names the exact
// object this reconcile classified, not merely its name. Delete locates its target by name
// alone, so without the precondition a snapshot that lagged across a whole
// delete-recreate-ready cycle would delete the replacement instead. The fake client does not
// enforce UID preconditions, so what is asserted here is what we pass; turning a mismatch into
// a 409 is the API server's contract.
func TestRecreateDeleteCarriesUIDPrecondition(t *testing.T) {
	scheme := rbgsTestScheme(t)

	oldRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	newRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1)), MinReadySeconds: 10}}

	set := rollingTestSet("s", 1, newRoles, &workloadsv1alpha2.GroupSetRolloutStrategy{})
	outdated := rollingTestChild("s", 0, oldRoles, true)
	outdated.UID = "uid-this-reconcile-classified"

	var seenUIDs []types.UID
	var seenPolicies []metav1.DeletionPropagation
	base := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(set, outdated).
		WithStatusSubresource(&workloadsv1alpha2.RoleBasedGroupSet{}).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(
			ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.DeleteOption,
		) error {
			do := client.DeleteOptions{}
			do.ApplyOptions(opts)
			if do.Preconditions != nil && do.Preconditions.UID != nil {
				seenUIDs = append(seenUIDs, *do.Preconditions.UID)
			}
			if do.PropagationPolicy != nil {
				seenPolicies = append(seenPolicies, *do.PropagationPolicy)
			}
			return inner.Delete(ctx, obj, opts...)
		},
	})
	r := &RoleBasedGroupSetReconciler{client: c, scheme: scheme, recorder: record.NewFakeRecorder(100)}

	reconcileSet(t, r, "s")

	assert.Equal(t, []types.UID{"uid-this-reconcile-classified"}, seenUIDs,
		"the recreate delete must pin the UID of the group it reasoned about")
	assert.Equal(t, []metav1.DeletionPropagation{metav1.DeletePropagationForeground}, seenPolicies,
		"the recreate delete must wait for the whole dependent chain")
}

// TestRollingUpdate_ReplicasOnlyChangeIsScaledInPlace verifies the exemption: when the only
// template diff is role replicas, groups are scaled in place instead of recreated.
func TestRollingUpdate_ReplicasOnlyChangeIsScaledInPlace(t *testing.T) {
	scheme := rbgsTestScheme(t)

	oldRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	newRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(4))}}

	set := rollingTestSet("s", 2, newRoles, &workloadsv1alpha2.GroupSetRolloutStrategy{})
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, oldRoles, true),
		rollingTestChild("s", 1, oldRoles, true),
	)

	reconcileSet(t, r, "s")

	// No group was deleted and both carry the new replica count.
	children := listChildren(t, r, "s")
	assert.Len(t, children, 2)
	for _, child := range children {
		assert.Equal(t, ptr.To(int32(4)), child.Spec.Roles[0].Replicas)
	}
}

// TestRollingUpdate_PartitionHoldsBackLowerOrdinals verifies that ordinals below the
// partition keep the previous template.
func TestRollingUpdate_PartitionHoldsBackLowerOrdinals(t *testing.T) {
	scheme := rbgsTestScheme(t)

	oldRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	newRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1)), MinReadySeconds: 10}}

	set := rollingTestSet("s", 3, newRoles, &workloadsv1alpha2.GroupSetRolloutStrategy{
		Partition: ptr.To(intstr.FromInt32(2)),
	})
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, oldRoles, true),
		rollingTestChild("s", 1, oldRoles, true),
		rollingTestChild("s", 2, oldRoles, true),
	)

	// Only ordinal 2 takes part in the rollout.
	reconcileSet(t, r, "s")
	assert.Len(t, listChildren(t, r, "s"), 2)

	// After the recreation is ready, the held-back ordinals are still untouched.
	reconcileSet(t, r, "s")
	markChildReady(t, r, "s-2")
	reconcileSet(t, r, "s")
	children := listChildren(t, r, "s")
	assert.Len(t, children, 3)
	assert.Equal(t, oldRoles, getChild(t, r, "s-0").Spec.Roles)
	assert.Equal(t, oldRoles, getChild(t, r, "s-1").Spec.Roles)
	assert.Equal(t, newRoles, getChild(t, r, "s-2").Spec.Roles)
}

// TestRollingUpdate_SurgeGroupsCreated verifies that surge groups are created at the ordinals
// above spec.replicas while a rollout is in flight.
func TestRollingUpdate_SurgeGroupsCreated(t *testing.T) {
	scheme := rbgsTestScheme(t)

	oldRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	newRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1)), MinReadySeconds: 10}}

	set := rollingTestSet("s", 2, newRoles, &workloadsv1alpha2.GroupSetRolloutStrategy{
		MaxSurge: ptr.To(intstr.FromInt32(1)),
	})
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, oldRoles, true),
		rollingTestChild("s", 1, oldRoles, true),
	)

	reconcileSet(t, r, "s")

	surge := getChild(t, r, "s-2")
	assert.Equal(t, "2", surge.Labels[constants.GroupSetIndexLabelKey])
	assert.Equal(t, newRoles, surge.Spec.Roles)
	// The unavailability budget still allows one base deletion alongside the not-yet-ready
	// surge group.
	children := listChildren(t, r, "s")
	assert.Len(t, children, 2) // s-0 and the surge group; s-1 was deleted
}

// TestRollingUpdate_SurgeKeptUntilRecreatedGroupIsReady verifies that surge capacity survives
// until every base group is serving. A recreated group matches the template the moment it
// exists, so gating reclamation on template equality alone withdraws the surge groups exactly
// when availability is at its lowest and drops the set below replicas-maxUnavailable.
func TestRollingUpdate_SurgeKeptUntilRecreatedGroupIsReady(t *testing.T) {
	scheme := rbgsTestScheme(t)

	roles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}

	set := rollingTestSet("s", 2, roles, &workloadsv1alpha2.GroupSetRolloutStrategy{
		MaxSurge: ptr.To(intstr.FromInt32(1)),
	})
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, roles, false), // recreated, on the new template, not serving yet
		rollingTestChild("s", 1, roles, true),
		rollingTestChild("s", 2, roles, true), // surge
	)

	reconcileSet(t, r, "s")
	assert.Len(t, listChildren(t, r, "s"), 3, "surge must keep serving while s-0 comes up")

	markChildReady(t, r, "s-0")
	reconcileSet(t, r, "s")
	assert.Len(t, listChildren(t, r, "s"), 2, "surge is reclaimed once every base group is ready")
}

// TestRollingUpdate_RolloutCompleteRequiresReady verifies that the Rolling condition keeps
// reporting RolloutInProgress while a group on the new template is still not serving. Without
// the readiness check it would claim RolloutComplete next to a Ready condition saying the
// opposite.
func TestRollingUpdate_RolloutCompleteRequiresReady(t *testing.T) {
	scheme := rbgsTestScheme(t)

	roles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}

	set := rollingTestSet("s", 2, roles, &workloadsv1alpha2.GroupSetRolloutStrategy{})
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, roles, false),
		rollingTestChild("s", 1, roles, true),
	)

	rollingCondition := func() *metav1.Condition {
		return meta.FindStatusCondition(
			getSet(t, r, "s").Status.Conditions, string(workloadsv1alpha2.RoleBasedGroupSetRolling),
		)
	}

	reconcileSet(t, r, "s")
	cond := rollingCondition()
	assert.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "RolloutInProgress", cond.Reason)

	markChildReady(t, r, "s-0")
	reconcileSet(t, r, "s")
	cond = rollingCondition()
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, "RolloutComplete", cond.Reason)
}

// TestRoleBasedGroupSetReconciler_TerminatingChildIsNotCountedReady verifies that a group being
// deleted stays out of readyReplicas and the Ready condition. Its own Ready condition remains
// true while its Pods work through preStop hooks and the termination grace period, so counting
// it would report full availability for that whole window even though it is going away.
func TestRoleBasedGroupSetReconciler_TerminatingChildIsNotCountedReady(t *testing.T) {
	scheme := rbgsTestScheme(t)

	roles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	set := rollingTestSet("s", 2, roles, &workloadsv1alpha2.GroupSetRolloutStrategy{})

	terminating := rollingTestChild("s", 1, roles, true)
	// The finalizer is what keeps an object with a deletionTimestamp alive in the fake client,
	// which is exactly the state foreground propagation produces while the Pods terminate.
	terminating.Finalizers = []string{"test/keep-alive"}
	now := metav1.Now()
	terminating.DeletionTimestamp = &now

	r := newRollingTestReconciler(scheme, set, rollingTestChild("s", 0, roles, true), terminating)
	reconcileSet(t, r, "s")

	status := getSet(t, r, "s").Status
	assert.Equal(t, int32(2), status.Replicas)
	assert.Equal(t, int32(1), status.ReadyReplicas, "the terminating group must not count as ready")

	ready := meta.FindStatusCondition(status.Conditions, string(workloadsv1alpha2.RoleBasedGroupSetReady))
	assert.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, "ReplicasNotReady", ready.Reason)
}

// TestRollingUpdate_PausedFreezesRollout verifies that a paused rollout deletes nothing.
func TestRollingUpdate_PausedFreezesRollout(t *testing.T) {
	scheme := rbgsTestScheme(t)

	oldRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	newRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1)), MinReadySeconds: 10}}

	set := rollingTestSet("s", 2, newRoles, &workloadsv1alpha2.GroupSetRolloutStrategy{
		Paused: true,
	})
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, oldRoles, true),
		rollingTestChild("s", 1, oldRoles, true),
	)

	reconcileSet(t, r, "s")

	children := listChildren(t, r, "s")
	assert.Len(t, children, 2)
	for _, child := range children {
		assert.Equal(t, oldRoles, child.Spec.Roles)
	}
}

// TestStaticUpdate_AllGroupsUpdatedInOnePass verifies the behavior for sets without a rollout
// strategy (including every set created before the feature existed): every outdated group is
// updated in place within a single reconcile, nothing is deleted.
func TestStaticUpdate_AllGroupsUpdatedInOnePass(t *testing.T) {
	scheme := rbgsTestScheme(t)

	oldRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1))}}
	newRoles := []workloadsv1alpha2.RoleSpec{{Name: "worker", Replicas: ptr.To(int32(1)), MinReadySeconds: 10}}

	set := rollingTestSet("s", 3, newRoles, &workloadsv1alpha2.GroupSetRolloutStrategy{})
	set.Spec.RolloutStrategy = nil
	r := newRollingTestReconciler(scheme, set,
		rollingTestChild("s", 0, oldRoles, true),
		rollingTestChild("s", 1, oldRoles, true),
		rollingTestChild("s", 2, oldRoles, true),
	)

	reconcileSet(t, r, "s")

	children := listChildren(t, r, "s")
	assert.Len(t, children, 3)
	for _, child := range children {
		assert.Equal(t, newRoles, child.Spec.Roles)
	}
}

func TestOnlyReplicasChanged(t *testing.T) {
	r := &RoleBasedGroupSetReconciler{}
	role := func(name string, replicas int32) workloadsv1alpha2.RoleSpec {
		return workloadsv1alpha2.RoleSpec{Name: name, Replicas: ptr.To(replicas)}
	}

	// Only replicas differ, order-insensitive.
	assert.True(t, r.onlyReplicasChanged(
		[]workloadsv1alpha2.RoleSpec{role("a", 1), role("b", 2)},
		[]workloadsv1alpha2.RoleSpec{role("b", 4), role("a", 3)},
	))
	// Identical roles vacuously qualify; callers gate on rolesEqual first.
	assert.True(t, r.onlyReplicasChanged(
		[]workloadsv1alpha2.RoleSpec{role("a", 1)},
		[]workloadsv1alpha2.RoleSpec{role("a", 1)},
	))
	// Role count differs.
	assert.False(t, r.onlyReplicasChanged(
		[]workloadsv1alpha2.RoleSpec{role("a", 1)},
		[]workloadsv1alpha2.RoleSpec{role("a", 1), role("b", 1)},
	))
	// Role renamed.
	assert.False(t, r.onlyReplicasChanged(
		[]workloadsv1alpha2.RoleSpec{role("a", 1)},
		[]workloadsv1alpha2.RoleSpec{role("z", 1)},
	))
	// Another field changed alongside replicas.
	other := role("a", 3)
	other.MinReadySeconds = 5
	assert.False(t, r.onlyReplicasChanged(
		[]workloadsv1alpha2.RoleSpec{role("a", 1)},
		[]workloadsv1alpha2.RoleSpec{other},
	))
}

func TestResolveRollingParams(t *testing.T) {
	// No rollout strategy at all.
	set := &workloadsv1alpha2.RoleBasedGroupSet{
		Spec: workloadsv1alpha2.RoleBasedGroupSetSpec{Replicas: ptr.To(int32(3))},
	}
	assert.Equal(t, rollingParams{maxUnavailable: 1}, resolveRollingParams(set))

	// Strategy present but no rollingUpdate block.
	set = rollingTestSet("s", 3, nil, nil)
	assert.Equal(t, rollingParams{maxUnavailable: 1}, resolveRollingParams(set))

	// Percentages: maxSurge rounds up, maxUnavailable rounds down while surge > 0,
	// partition rounds down.
	set = rollingTestSet("s", 10, nil, &workloadsv1alpha2.GroupSetRolloutStrategy{
		MaxSurge:       ptr.To(intstr.FromString("20%")),
		MaxUnavailable: ptr.To(intstr.FromString("25%")),
		Partition:      ptr.To(intstr.FromString("30%")),
	})
	assert.Equal(t, rollingParams{partition: 3, maxUnavailable: 2, maxSurge: 2}, resolveRollingParams(set))

	// maxUnavailable rounds up when maxSurge is 0.
	set = rollingTestSet("s", 10, nil, &workloadsv1alpha2.GroupSetRolloutStrategy{
		MaxUnavailable: ptr.To(intstr.FromString("21%")),
	})
	assert.Equal(t, rollingParams{maxUnavailable: 3}, resolveRollingParams(set))

	// A resolved maxUnavailable of 0 is kept when a surge budget backs it: the ready surge
	// groups supply the capacity, so flooring to 1 would let the rollout drop a base group
	// the user explicitly asked to keep.
	set = rollingTestSet("s", 10, nil, &workloadsv1alpha2.GroupSetRolloutStrategy{
		MaxSurge:       ptr.To(intstr.FromInt32(1)),
		MaxUnavailable: ptr.To(intstr.FromString("1%")),
	})
	assert.Equal(t, rollingParams{maxUnavailable: 0, maxSurge: 1}, resolveRollingParams(set))

	// Without a surge budget the same resolved 0 is floored to 1, otherwise no group could
	// ever be recreated and the rollout would stall.
	set = rollingTestSet("s", 10, nil, &workloadsv1alpha2.GroupSetRolloutStrategy{
		MaxUnavailable: ptr.To(intstr.FromString("1%")),
	})
	assert.Equal(t, rollingParams{maxUnavailable: 1}, resolveRollingParams(set))

	// Partition is clamped to replicas (admission normally rejects larger values).
	set = rollingTestSet("s", 2, nil, &workloadsv1alpha2.GroupSetRolloutStrategy{
		Partition: ptr.To(intstr.FromInt32(5)),
	})
	assert.Equal(t, rollingParams{partition: 2, maxUnavailable: 1}, resolveRollingParams(set))
}

// TestSyncRBGMetadataKeepsSystemKeys covers the loop two controllers got into. The
// RoleBasedGroup controller records discovery-config-mode on its own object; a template sync
// that rebuilt the whole annotation map deleted it, so that controller wrote it again, so this
// one saw drift again, and the child was updated forever while its downstream workloads
// re-hashed and its readiness flickered.
func TestSyncRBGMetadataKeepsSystemKeys(t *testing.T) {
	r := &RoleBasedGroupSetReconciler{}
	rbgset := &workloadsv1alpha2.RoleBasedGroupSet{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "default"},
	}
	rbg := &workloadsv1alpha2.RoleBasedGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s-0",
			Namespace: "default",
			Labels: map[string]string{
				constants.GroupSetNameLabelKey:  "s",
				constants.GroupSetIndexLabelKey: "0",
			},
			Annotations: map[string]string{
				constants.DiscoveryConfigModeAnnotationKey: "refine",
				"example.com/dropped":                      "gone",
			},
		},
	}

	r.syncRBGMetadata(rbgset, rbg)

	assert.Equal(t, "refine", rbg.Annotations[constants.DiscoveryConfigModeAnnotationKey],
		"an annotation another controller owns must survive a template sync")
	assert.NotContains(t, rbg.Annotations, "example.com/dropped",
		"a user annotation the template no longer specifies is still dropped")
	assert.Equal(t, "s", rbg.Labels[constants.GroupSetNameLabelKey])
	assert.Equal(t, "0", rbg.Labels[constants.GroupSetIndexLabelKey])

	// The point of the whole exercise: after one sync the predicates agree, so the next
	// reconcile has nothing to do and the two controllers settle.
	assert.False(t, r.needsTemplateAnnotationUpdate(rbgset, rbg))
	assert.False(t, r.needsTemplateLabelUpdate(rbgset, rbg))
}

func TestClassifyChildren(t *testing.T) {
	list := &workloadsv1alpha2.RoleBasedGroupList{
		Items: []workloadsv1alpha2.RoleBasedGroup{
			*rollingTestChild("s", 0, nil, true),
			*rollingTestChild("s", 2, nil, true),
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "bad-index",
					Labels: map[string]string{constants.GroupSetIndexLabelKey: "abc"},
				},
			},
			{ObjectMeta: metav1.ObjectMeta{Name: "no-index"}},
		},
	}

	children := classifyChildren(list, 2)
	assert.Len(t, children.base, 1)
	assert.Contains(t, children.base, 0)
	assert.Len(t, children.surge, 1)
	assert.Contains(t, children.surge, 2)
	assert.Len(t, children.invalid, 2)
}
