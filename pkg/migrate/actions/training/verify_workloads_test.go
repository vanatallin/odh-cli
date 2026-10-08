package training_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blang/semver/v4"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/opendatahub-io/odh-cli/pkg/constants"
	"github.com/opendatahub-io/odh-cli/pkg/migrate/action"
	"github.com/opendatahub-io/odh-cli/pkg/migrate/action/result"
	trainingaction "github.com/opendatahub-io/odh-cli/pkg/migrate/actions/training"
	"github.com/opendatahub-io/odh-cli/pkg/resources"
	"github.com/opendatahub-io/odh-cli/pkg/util/client"
	"github.com/opendatahub-io/odh-cli/pkg/util/iostreams"

	. "github.com/onsi/gomega"
)

const testTimestamp = "2025-01-01T00:00:00Z"

const (
	testCurrent2x = "2.25.0"
	testTarget3x  = "3.0.0"
	testCurrent35 = "3.5.0"
	testTarget36  = "3.6.0"
)

//nolint:gochecknoglobals // Test fixture
var trainingOperatorCRType = resources.ComponentCRResourceTypes[constants.ComponentTrainingOperator]

//nolint:gochecknoglobals // Test fixture
var trainingOperatorCRGroupResource = schema.GroupResource{
	Group:    trainingOperatorCRType.Group,
	Resource: trainingOperatorCRType.Resource,
}

//nolint:gochecknoglobals // Test fixture
var listKinds = map[schema.GroupVersionResource]string{
	resources.PyTorchJob.GVR():         resources.PyTorchJob.ListKind(),
	resources.TFJob.GVR():              resources.TFJob.ListKind(),
	resources.MPIJob.GVR():             resources.MPIJob.ListKind(),
	resources.XGBoostJob.GVR():         resources.XGBoostJob.ListKind(),
	resources.TrainJob.GVR():           resources.TrainJob.ListKind(),
	resources.DataScienceCluster.GVR(): resources.DataScienceCluster.ListKind(),
	trainingOperatorCRType.GVR():       trainingOperatorCRType.ListKind(),
}

func newTrainingJob(rt resources.ResourceType, name, namespace string, conditions []any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": rt.APIVersion(),
		"kind":       rt.Kind,
		"metadata": map[string]any{
			"name":              name,
			"namespace":         namespace,
			"creationTimestamp": testTimestamp,
		},
	}

	if conditions != nil {
		obj["status"] = map[string]any{
			"conditions": conditions,
		}
	}

	return &unstructured.Unstructured{Object: obj}
}

func newDSC(trainingoperatorState string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": resources.DataScienceCluster.APIVersion(),
		"kind":       resources.DataScienceCluster.Kind,
		"metadata": map[string]any{
			"name": "default-dsc",
		},
		"spec": map[string]any{
			"components": map[string]any{
				"trainingoperator": map[string]any{
					"managementState": trainingoperatorState,
				},
			},
		},
	}}
}

func newTrainingOperatorCR(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": trainingOperatorCRType.APIVersion(),
		"kind":       trainingOperatorCRType.Kind,
		"metadata": map[string]any{
			"name": name,
		},
	}}
}

func succeededConditions() []any {
	return []any{
		map[string]any{"type": "Created", "status": "True"},
		map[string]any{"type": "Running", "status": "False"},
		map[string]any{"type": "Succeeded", "status": "True"},
	}
}

func runningConditions() []any {
	return []any{
		map[string]any{"type": "Created", "status": "True"},
		map[string]any{"type": "Running", "status": "True"},
	}
}

func failedConditions() []any {
	return []any{
		map[string]any{"type": "Created", "status": "True"},
		map[string]any{"type": "Running", "status": "False"},
		map[string]any{"type": "Failed", "status": "True"},
	}
}

func createdConditions() []any {
	return []any{
		map[string]any{"type": "Created", "status": "True"},
	}
}

func newTestTarget(objects ...runtime.Object) action.Target {
	return newTestTargetForVersions(testCurrent2x, testTarget3x, objects...)
}

func newTestTargetForVersions(current string, target string, objects ...runtime.Object) action.Target {
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objects...)

	return newTargetFromDynamicClient(dynamicClient, current, target)
}

// newTargetFromDynamicClient builds a target from a caller-configured fake
// dynamic client so tests can prepend reactors that simulate cluster states a
// plain object fixture cannot express.
func newTargetFromDynamicClient(dynamicClient *dynamicfake.FakeDynamicClient, current string, target string) action.Target {
	testClient := client.NewForTesting(client.TestClientConfig{
		Dynamic: dynamicClient,
	})

	v := semver.MustParse(current)
	tv := semver.MustParse(target)

	return action.Target{
		Client:         testClient,
		CurrentVersion: &v,
		TargetVersion:  &tv,
		DryRun:         false,
		SkipConfirm:    true,
		Recorder:       action.NewRootRecorder(),
		IO:             iostreams.NewIOStreams(nil, &bytes.Buffer{}, &bytes.Buffer{}),
	}
}

// newTestTargetWithoutCRD simulates a cluster that does not serve the given
// resource: listing it fails with a not-found error, matching how
// IsResourceTypeNotFound degrades on clusters without the CRD installed. A
// reactor is required because the fake client panics on list kinds that are
// missing from the listKinds map.
func newTestTargetWithoutCRD(current string, target string, gr schema.GroupResource, objects ...runtime.Object) action.Target {
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objects...)
	dynamicClient.PrependReactor("list", gr.Resource, func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(gr, "")
	})

	return newTargetFromDynamicClient(dynamicClient, current, target)
}

func TestVerifyWorkloadsAction_Metadata(t *testing.T) {
	g := NewWithT(t)

	a := &trainingaction.VerifyWorkloadsAction{}
	g.Expect(a.ID()).To(Equal("training.verify-workloads"))
	g.Expect(a.Group()).To(Equal(action.GroupValidation))
	g.Expect(a.Phase()).To(Equal(action.PhasePreUpgrade))
	g.Expect(a.Prepare()).To(BeNil())
	g.Expect(a.Run()).ToNot(BeNil())
}

func TestVerifyWorkloadsAction_CanApply(t *testing.T) {
	t.Run("applies when current version is 2.x", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("2.25.0")
		tv := semver.MustParse("3.0.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeTrue())
	})

	t.Run("applies when upgrading from 3.5 to 3.6", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("3.5.0")
		tv := semver.MustParse("3.6.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeTrue())
	})

	t.Run("applies when upgrading from 2.x directly to 3.6", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("2.25.0")
		tv := semver.MustParse("3.6.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeTrue())
	})

	t.Run("does not apply when already on 3.6", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("3.6.1")
		tv := semver.MustParse("3.7.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeFalse())
	})

	t.Run("applies when upgrading between 3.x minors below 3.6", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("3.0.0")
		tv := semver.MustParse("3.1.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeTrue())
	})

	t.Run("does not apply when current and target versions are equal", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("3.5.0")
		tv := semver.MustParse("3.5.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeFalse())
	})

	t.Run("does not apply when current version is 4.x", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("4.0.0")
		tv := semver.MustParse("4.1.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeFalse())
	})

	t.Run("does not apply when current version is nil", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		tv := semver.MustParse("3.6.0")

		g.Expect(a.CanApply(action.Target{
			TargetVersion: &tv,
		})).To(BeFalse())
	})

	t.Run("does not apply when target version is nil", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("3.5.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
		})).To(BeFalse())
	})

	t.Run("applies when upgrading between 2.x versions", func(t *testing.T) {
		g := NewWithT(t)

		a := &trainingaction.VerifyWorkloadsAction{}
		v := semver.MustParse("2.16.0")
		tv := semver.MustParse("2.25.0")

		g.Expect(a.CanApply(action.Target{
			CurrentVersion: &v,
			TargetVersion:  &tv,
		})).To(BeTrue())
	})
}

func TestVerifyWorkloadsAction_RunWarnsOnActiveJobs(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	running := newTrainingJob(resources.PyTorchJob, "running-job", "ns-a", runningConditions())
	created := newTrainingJob(resources.TFJob, "created-job", "ns-b", createdConditions())

	target := newTestTarget(running, created)

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(actionResult).ToNot(BeNil())
	g.Expect(actionResult.Status.Completed).To(BeTrue())
	g.Expect(actionResult.HasFailedSteps()).To(BeFalse())
	g.Expect(actionResult.HasWarningSteps()).To(BeTrue())

	readiness := findStep(actionResult.Status.Steps, "migration-readiness")
	g.Expect(readiness).ToNot(BeNil())
	g.Expect(readiness.Status).To(Equal(result.StepWarning))
	g.Expect(readiness.Message).To(And(
		ContainSubstring("2 active"),
		ContainSubstring("does not block the upgrade"),
	))
	g.Expect(readiness.Details["active"]).To(Equal(2))
	g.Expect(readiness.Details["completed"]).To(Equal(0))
}

func TestVerifyWorkloadsAction_RunNoActiveJobs(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	succeeded := newTrainingJob(resources.PyTorchJob, "done-job", "ns-a", succeededConditions())
	failed := newTrainingJob(resources.TFJob, "failed-job", "ns-a", failedConditions())

	target := newTestTarget(succeeded, failed)

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(actionResult).ToNot(BeNil())
	g.Expect(actionResult.HasWarningSteps()).To(BeFalse())

	readiness := findStep(actionResult.Status.Steps, "migration-readiness")
	g.Expect(readiness).ToNot(BeNil())
	g.Expect(readiness.Status).To(Equal(result.StepCompleted))
	g.Expect(readiness.Message).To(ContainSubstring("TrainingOperator has been removed"))
	g.Expect(readiness.Details["active"]).To(Equal(0))
	g.Expect(readiness.Details["completed"]).To(Equal(2))
}

func TestVerifyWorkloadsAction_RunMixed(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	running := newTrainingJob(resources.PyTorchJob, "active-1", "ns-a", runningConditions())
	succeeded1 := newTrainingJob(resources.PyTorchJob, "done-1", "ns-a", succeededConditions())
	succeeded2 := newTrainingJob(resources.TFJob, "done-2", "ns-a", succeededConditions())
	failed := newTrainingJob(resources.MPIJob, "failed-1", "ns-b", failedConditions())

	target := newTestTarget(running, succeeded1, succeeded2, failed)

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())

	readiness := findStep(actionResult.Status.Steps, "migration-readiness")
	g.Expect(readiness).ToNot(BeNil())
	g.Expect(readiness.Status).To(Equal(result.StepWarning))
	g.Expect(readiness.Details["active"]).To(Equal(1))
	g.Expect(readiness.Details["completed"]).To(Equal(3))

	summary := findStep(actionResult.Status.Steps, "summary")
	g.Expect(summary).ToNot(BeNil())
	g.Expect(summary.Details["total"]).To(Equal(4))
	g.Expect(summary.Details["active"]).To(Equal(1))
	g.Expect(summary.Details["completed"]).To(Equal(3))
}

func TestVerifyWorkloadsAction_RunTrainJobCRDInstalled(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	target := newTestTarget()

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())

	crdStep := findStep(actionResult.Status.Steps, "trainjob-crd")
	g.Expect(crdStep).ToNot(BeNil())
	g.Expect(crdStep.Status).To(Equal(result.StepCompleted))
	g.Expect(crdStep.Details["trainjobCRDInstalled"]).To(BeTrue())
	g.Expect(crdStep.Message).To(ContainSubstring("v2 API available"))

	summary := findStep(actionResult.Status.Steps, "summary")
	g.Expect(summary).ToNot(BeNil())
	g.Expect(summary.Details["trainjobCRDInstalled"]).To(BeTrue())
}

func TestVerifyWorkloadsAction_RunTrainJobCRDNotInstalled(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	scheme := runtime.NewScheme()
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
	dynamicClient.PrependReactor("list", "trainjobs", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(
			schema.GroupResource{Group: resources.TrainJob.Group, Resource: resources.TrainJob.Resource},
			"",
		)
	})

	testClient := client.NewForTesting(client.TestClientConfig{
		Dynamic: dynamicClient,
	})

	v := semver.MustParse(testCurrent2x)
	tv := semver.MustParse(testTarget3x)

	target := action.Target{
		Client:         testClient,
		CurrentVersion: &v,
		TargetVersion:  &tv,
		Recorder:       action.NewRootRecorder(),
		IO:             iostreams.NewIOStreams(nil, &bytes.Buffer{}, &bytes.Buffer{}),
	}

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())

	crdStep := findStep(actionResult.Status.Steps, "trainjob-crd")
	g.Expect(crdStep).ToNot(BeNil())
	g.Expect(crdStep.Status).To(Equal(result.StepCompleted))
	g.Expect(crdStep.Details["trainjobCRDInstalled"]).To(BeFalse())
	g.Expect(crdStep.Message).To(ContainSubstring("not installed"))
}

func TestVerifyWorkloadsAction_RunTrainJobCRDCheckError(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	scheme := runtime.NewScheme()
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
	dynamicClient.PrependReactor("list", "trainjobs", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})

	testClient := client.NewForTesting(client.TestClientConfig{
		Dynamic: dynamicClient,
	})

	v := semver.MustParse(testCurrent2x)
	tv := semver.MustParse(testTarget3x)

	target := action.Target{
		Client:         testClient,
		CurrentVersion: &v,
		TargetVersion:  &tv,
		Recorder:       action.NewRootRecorder(),
		IO:             iostreams.NewIOStreams(nil, &bytes.Buffer{}, &bytes.Buffer{}),
	}

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("checking TrainJob CRD"))

	crdStep := findStep(actionResult.Status.Steps, "trainjob-crd")
	g.Expect(crdStep).ToNot(BeNil())
	g.Expect(crdStep.Status).To(Equal(result.StepFailed))
	g.Expect(crdStep.Details["trainjobCRDInstalled"]).To(BeFalse())
}

func TestVerifyWorkloadsAction_RunMigrationMap(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	pytorch := newTrainingJob(resources.PyTorchJob, "pt-1", "ns-a", succeededConditions())
	tfjob := newTrainingJob(resources.TFJob, "tf-1", "ns-a", succeededConditions())

	target := newTestTarget(pytorch, tfjob)

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())

	summary := findStep(actionResult.Status.Steps, "summary")
	g.Expect(summary).ToNot(BeNil())

	migrationMap, ok := summary.Details["migrationMap"].(map[string]string)
	g.Expect(ok).To(BeTrue())
	g.Expect(migrationMap).To(HaveLen(2))
	g.Expect(migrationMap["PyTorchJob"]).To(ContainSubstring("torch"))
	g.Expect(migrationMap["TFJob"]).To(ContainSubstring("tensorflow"))
	g.Expect(migrationMap).ToNot(HaveKey("MPIJob"))
	g.Expect(migrationMap).ToNot(HaveKey("XGBoostJob"))
}

func TestVerifyWorkloadsAction_RunNoWorkloads(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	target := newTestTarget()

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(actionResult).ToNot(BeNil())
	g.Expect(actionResult.Status.Completed).To(BeTrue())
	g.Expect(actionResult.HasWarningSteps()).To(BeFalse())

	readiness := findStep(actionResult.Status.Steps, "migration-readiness")
	g.Expect(readiness).ToNot(BeNil())
	g.Expect(readiness.Status).To(Equal(result.StepCompleted))
	g.Expect(readiness.Message).To(ContainSubstring("nothing to migrate"))

	summary := findStep(actionResult.Status.Steps, "summary")
	g.Expect(summary).ToNot(BeNil())
	g.Expect(summary.Details["total"]).To(Equal(0))
}

func TestVerifyWorkloadsAction_RunAllListsFail(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	scheme := runtime.NewScheme()
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
	dynamicClient.PrependReactor("list", "*", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})
	dynamicClient.PrependReactor("list", "trainjobs", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.UnstructuredList{}, nil
	})
	dynamicClient.PrependReactor("list", "datascienceclusters", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.UnstructuredList{}, nil
	})
	dynamicClient.PrependReactor("list", "trainingoperators", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.UnstructuredList{}, nil
	})

	testClient := client.NewForTesting(client.TestClientConfig{
		Dynamic: dynamicClient,
	})

	v := semver.MustParse(testCurrent2x)
	tv := semver.MustParse(testTarget3x)

	target := action.Target{
		Client:         testClient,
		CurrentVersion: &v,
		TargetVersion:  &tv,
		Recorder:       action.NewRootRecorder(),
		IO:             iostreams.NewIOStreams(nil, &bytes.Buffer{}, &bytes.Buffer{}),
	}

	a := &trainingaction.VerifyWorkloadsAction{}
	_, err := a.Run().Execute(ctx, target)

	g.Expect(err).To(HaveOccurred())
	g.Expect(err.Error()).To(ContainSubstring("failed to list any training workload types"))
}

func TestVerifyWorkloadsAction_RunPartialFailure(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	tfjob := newTrainingJob(resources.TFJob, "tf-1", "ns-a", succeededConditions())

	scheme := runtime.NewScheme()
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, tfjob)
	dynamicClient.PrependReactor("list", "pytorchjobs", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})

	testClient := client.NewForTesting(client.TestClientConfig{
		Dynamic: dynamicClient,
	})

	v := semver.MustParse(testCurrent2x)
	tv := semver.MustParse(testTarget3x)

	target := action.Target{
		Client:         testClient,
		CurrentVersion: &v,
		TargetVersion:  &tv,
		Recorder:       action.NewRootRecorder(),
		IO:             iostreams.NewIOStreams(nil, &bytes.Buffer{}, &bytes.Buffer{}),
	}

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Execute(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(actionResult).ToNot(BeNil())
	g.Expect(actionResult.Status.Completed).To(BeTrue())

	pytorchStep := findStep(actionResult.Status.Steps, "pytorchjobs")
	g.Expect(pytorchStep).ToNot(BeNil())
	g.Expect(pytorchStep.Status).To(Equal(result.StepFailed))

	summary := findStep(actionResult.Status.Steps, "summary")
	g.Expect(summary).ToNot(BeNil())
	g.Expect(summary.Details["total"]).To(Equal(1))
}

func TestVerifyWorkloadsAction_TrainingOperatorStatus(t *testing.T) {
	t.Run("warns when TrainingOperator is enabled and upgrading to 3.6", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		target := newTestTargetForVersions(testCurrent35, testTarget36,
			newDSC("Managed"),
			newTrainingOperatorCR("training-operator"),
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(actionResult.Status.Completed).To(BeTrue())
		g.Expect(actionResult.HasWarningSteps()).To(BeTrue())
		g.Expect(actionResult.HasFailedSteps()).To(BeFalse())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepWarning))
		g.Expect(statusStep.Message).To(ContainSubstring("removed in RHOAI 3.6"))
		g.Expect(statusStep.Message).To(ContainSubstring("no longer be managed"))
		g.Expect(statusStep.Message).To(ContainSubstring("managementState to 'Removed'"))
		g.Expect(statusStep.Message).To(ContainSubstring("does not block the upgrade"))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal("Managed"))
		g.Expect(statusStep.Details["trainingoperatorCRs"]).To(Equal(1))
	})

	t.Run("warns about inconsistent state when managed without component CRs", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		target := newTestTargetForVersions(testCurrent35, testTarget36, newDSC("Managed"))

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepWarning))
		g.Expect(statusStep.Message).To(And(
			ContainSubstring("inconsistent state"),
			ContainSubstring("does not block the upgrade"),
			ContainSubstring("managementState to 'Removed'"),
		))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal("Managed"))
		g.Expect(statusStep.Details["trainingoperatorCRs"]).To(Equal(0))
	})

	t.Run("warns when TrainingOperator CRs remain after removal", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		target := newTestTargetForVersions(
			testCurrent35,
			testTarget36,
			newDSC("Removed"),
			newTrainingOperatorCR("trainingoperator"),
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepWarning))
		g.Expect(statusStep.Message).To(ContainSubstring("still exist"))
		g.Expect(statusStep.Message).To(ContainSubstring("trainingoperator"))
		g.Expect(statusStep.Message).To(ContainSubstring("does not block the upgrade"))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal("Removed"))
		g.Expect(statusStep.Details["trainingoperatorCRs"]).To(Equal(1))
	})

	t.Run("completes when TrainingOperator is removed", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		target := newTestTargetForVersions(testCurrent35, testTarget36, newDSC("Removed"))

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepCompleted))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal("Removed"))
		g.Expect(statusStep.Details["trainingoperatorCRs"]).To(Equal(0))
	})

	t.Run("does not warn about removal for targets below 3.6", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		target := newTestTargetForVersions(testCurrent2x, testTarget3x, newDSC("Managed"))

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepCompleted))
		g.Expect(statusStep.Message).To(ContainSubstring("Managed"))
	})

	t.Run("completes when no DataScienceCluster exists", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		target := newTestTargetForVersions(testCurrent35, testTarget36)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepCompleted))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal("Removed"))
	})
}

func TestVerifyWorkloadsAction_RunClusterStates(t *testing.T) {
	t.Run("warns on every step when TrainingOperator is managed with active jobs", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		running := newTrainingJob(resources.PyTorchJob, "run-1", "ns-a", runningConditions())
		created := newTrainingJob(resources.TFJob, "create-1", "ns-b", createdConditions())

		target := newTestTargetForVersions(testCurrent35, testTarget36,
			newDSC(constants.ManagementStateManaged),
			newTrainingOperatorCR("training-operator"),
			running, created,
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(actionResult.Status.Completed).To(BeTrue())
		g.Expect(actionResult.HasFailedSteps()).To(BeFalse())
		g.Expect(actionResult.HasWarningSteps()).To(BeTrue())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepWarning))

		g.Expect(statusStep.Message).To(And(
			ContainSubstring("removed in RHOAI 3.6"),
			ContainSubstring("does not block the upgrade"),
			ContainSubstring("managementState to 'Removed'"),
		))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal(constants.ManagementStateManaged))
		g.Expect(statusStep.Details["trainingoperatorCRs"]).To(Equal(1))

		readiness := findStep(actionResult.Status.Steps, "migration-readiness")
		g.Expect(readiness).ToNot(BeNil())
		g.Expect(readiness.Status).To(Equal(result.StepWarning))
		g.Expect(readiness.Details["active"]).To(Equal(2))
		g.Expect(readiness.Details["completed"]).To(Equal(0))

		summary := findStep(actionResult.Status.Steps, "summary")
		g.Expect(summary).ToNot(BeNil())
		g.Expect(summary.Status).To(Equal(result.StepWarning))
		g.Expect(summary.Details["active"]).To(Equal(2))
	})

	t.Run("does not claim removal when managed and no jobs are active", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		succeeded := newTrainingJob(resources.PyTorchJob, "done-1", "ns-a", succeededConditions())
		failed := newTrainingJob(resources.MPIJob, "failed-1", "ns-b", failedConditions())

		target := newTestTargetForVersions(testCurrent35, testTarget36,
			newDSC(constants.ManagementStateManaged),
			newTrainingOperatorCR("training-operator"),
			succeeded, failed,
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		// The component is still enabled, so the status step keeps warning
		// and advises setting it to Removed before upgrading, even though
		// no jobs are active.
		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepWarning))
		g.Expect(statusStep.Message).To(And(
			ContainSubstring("removed in RHOAI 3.6"),
			ContainSubstring("does not block the upgrade"),
			ContainSubstring("managementState to 'Removed'"),
		))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal(constants.ManagementStateManaged))
		g.Expect(statusStep.Details["trainingoperatorCRs"]).To(Equal(1))

		// The job steps must not claim the operator was already removed:
		// completed jobs are normal on a managed cluster.
		readiness := findStep(actionResult.Status.Steps, "migration-readiness")
		g.Expect(readiness).ToNot(BeNil())
		g.Expect(readiness.Status).To(Equal(result.StepCompleted))
		g.Expect(readiness.Message).To(And(
			ContainSubstring("No active v1 jobs"),
			Not(ContainSubstring("has been removed")),
		))
		g.Expect(readiness.Details["active"]).To(Equal(0))
		g.Expect(readiness.Details["completed"]).To(Equal(2))

		summary := findStep(actionResult.Status.Steps, "summary")
		g.Expect(summary).ToNot(BeNil())
		g.Expect(summary.Status).To(Equal(result.StepCompleted))
		g.Expect(summary.Message).To(Not(ContainSubstring("has been removed")))

		// Warnings come from the status step, not from the job steps.
		g.Expect(actionResult.HasWarningSteps()).To(BeTrue())
	})

	t.Run("completes cleanly when TrainingOperator is removed and its CRD is gone", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		target := newTestTargetWithoutCRD(testCurrent35, testTarget36,
			trainingOperatorCRGroupResource,
			newDSC(constants.ManagementStateRemoved),
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(actionResult.Status.Completed).To(BeTrue())
		g.Expect(actionResult.HasWarningSteps()).To(BeFalse())
		g.Expect(actionResult.HasFailedSteps()).To(BeFalse())

		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepCompleted))
		g.Expect(statusStep.Details["trainingoperatorState"]).To(Equal(constants.ManagementStateRemoved))
		g.Expect(statusStep.Details["trainingoperatorCRs"]).To(Equal(0))

		readiness := findStep(actionResult.Status.Steps, "migration-readiness")
		g.Expect(readiness).ToNot(BeNil())
		g.Expect(readiness.Status).To(Equal(result.StepCompleted))
		g.Expect(readiness.Message).To(ContainSubstring("No v1 training workloads found"))

		summary := findStep(actionResult.Status.Steps, "summary")
		g.Expect(summary).ToNot(BeNil())
		g.Expect(summary.Details["total"]).To(Equal(0))
	})

	t.Run("reports leftover jobs as unmanaged when the operator is removed and its CRD is gone", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		succeeded := newTrainingJob(resources.PyTorchJob, "done-1", "ns-a", succeededConditions())

		target := newTestTargetWithoutCRD(testCurrent35, testTarget36,
			trainingOperatorCRGroupResource,
			newDSC(constants.ManagementStateRemoved),
			succeeded,
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		readiness := findStep(actionResult.Status.Steps, "migration-readiness")
		g.Expect(readiness).ToNot(BeNil())
		g.Expect(readiness.Status).To(Equal(result.StepCompleted))
		g.Expect(readiness.Message).To(ContainSubstring("TrainingOperator has been removed"))

		summary := findStep(actionResult.Status.Steps, "summary")
		g.Expect(summary).ToNot(BeNil())
		g.Expect(summary.Message).To(ContainSubstring("no longer managed"))
	})

	t.Run("does not claim removal when component CRs remain after disablement", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		succeeded := newTrainingJob(resources.PyTorchJob, "done-1", "ns-a", succeededConditions())

		target := newTestTargetForVersions(testCurrent35, testTarget36,
			newDSC(constants.ManagementStateRemoved),
			newTrainingOperatorCR("leftover-operator"),
			succeeded,
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())

		// Disabled with leftover CRs: the status step warns about the
		// incomplete cleanup, and the job steps stay silent about removal.
		statusStep := findStep(actionResult.Status.Steps, "trainingoperator-status")
		g.Expect(statusStep).ToNot(BeNil())
		g.Expect(statusStep.Status).To(Equal(result.StepWarning))

		g.Expect(statusStep.Message).To(And(
			ContainSubstring("does not block the upgrade"),
			ContainSubstring("consider cleaning them up"),
		))

		readiness := findStep(actionResult.Status.Steps, "migration-readiness")
		g.Expect(readiness).ToNot(BeNil())
		g.Expect(readiness.Message).To(Not(ContainSubstring("has been removed")))

		summary := findStep(actionResult.Status.Steps, "summary")
		g.Expect(summary).ToNot(BeNil())
		g.Expect(summary.Message).To(Not(ContainSubstring("has been removed")))
	})

	t.Run("completes without warnings when managed with no active jobs for targets below 3.6", func(t *testing.T) {
		g := NewWithT(t)
		ctx := t.Context()

		succeeded := newTrainingJob(resources.PyTorchJob, "done-1", "ns-a", succeededConditions())

		target := newTestTargetForVersions(testCurrent2x, testTarget3x,
			newDSC(constants.ManagementStateManaged),
			newTrainingOperatorCR("training-operator"),
			succeeded,
		)

		a := &trainingaction.VerifyWorkloadsAction{}
		actionResult, err := a.Run().Execute(ctx, target)

		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(actionResult.HasWarningSteps()).To(BeFalse())

		readiness := findStep(actionResult.Status.Steps, "migration-readiness")
		g.Expect(readiness).ToNot(BeNil())
		g.Expect(readiness.Status).To(Equal(result.StepCompleted))
		g.Expect(readiness.Message).To(ContainSubstring("No active v1 jobs"))
	})
}

func TestVerifyWorkloadsAction_ValidateSameAsExecute(t *testing.T) {
	g := NewWithT(t)
	ctx := t.Context()

	pytorch := newTrainingJob(resources.PyTorchJob, "pt-1", "ns-a", succeededConditions())
	target := newTestTarget(pytorch)

	a := &trainingaction.VerifyWorkloadsAction{}
	actionResult, err := a.Run().Validate(ctx, target)

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(actionResult).ToNot(BeNil())
	g.Expect(actionResult.Status.Completed).To(BeTrue())
}

func findStep(steps []result.ActionStep, name string) *result.ActionStep {
	for i := range steps {
		if steps[i].Name == name {
			return &steps[i]
		}
	}

	return nil
}
