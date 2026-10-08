package training

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/opendatahub-io/odh-cli/pkg/constants"
	"github.com/opendatahub-io/odh-cli/pkg/migrate/action"
	"github.com/opendatahub-io/odh-cli/pkg/migrate/action/result"
	"github.com/opendatahub-io/odh-cli/pkg/resources"
	"github.com/opendatahub-io/odh-cli/pkg/util/client"
	"github.com/opendatahub-io/odh-cli/pkg/util/components"
	"github.com/opendatahub-io/odh-cli/pkg/util/version"
)

// VerifyWorkloadsAction enumerates Kubeflow v1 training workloads and the
// TrainingOperator component state as a pre-upgrade check. Findings are
// reported as warnings only: the upgrade is never blocked, but the user is
// advised to drain v1 workloads and set trainingoperator to Removed before
// upgrading to RHOAI 3.6.
type VerifyWorkloadsAction struct{}

func (a *VerifyWorkloadsAction) ID() string          { return verifyWorkloadsID }
func (a *VerifyWorkloadsAction) Name() string        { return verifyWorkloadsName }
func (a *VerifyWorkloadsAction) Description() string { return verifyWorkloadsDescription }

func (a *VerifyWorkloadsAction) Group() action.ActionGroup { return action.GroupValidation }
func (a *VerifyWorkloadsAction) Phase() action.ActionPhase { return action.PhasePreUpgrade }

// CanApply matches upgrades from any 2.x or pre-3.6 3.x current version:
// 2.x clusters are on the Trainer v2 migration path, and TrainingOperator is
// removed in RHOAI 3.6. A target equal to the current version means no upgrade
// is happening and is excluded.
func (a *VerifyWorkloadsAction) CanApply(target action.Target) bool {
	if target.CurrentVersion == nil || target.TargetVersion == nil {
		return false
	}

	if target.CurrentVersion.EQ(*target.TargetVersion) {
		return false
	}

	//nolint:mnd // Version numbers 3.6 — TrainingOperator removed in RHOAI 3.6
	return target.CurrentVersion.Major == 2 ||
		(target.CurrentVersion.Major == 3 && !version.IsVersionAtLeast(target.CurrentVersion, 3, 6))
}

func (a *VerifyWorkloadsAction) Prepare() action.Task { return nil }
func (a *VerifyWorkloadsAction) Run() action.Task     { return &verifyWorkloadsTask{} }

type verifyWorkloadsTask struct{}

func (t *verifyWorkloadsTask) Validate(ctx context.Context, target action.Target) (*result.ActionResult, error) {
	return t.Execute(ctx, target)
}

func (t *verifyWorkloadsTask) Execute(ctx context.Context, target action.Target) (*result.ActionResult, error) {
	recorder := action.NewVerboseRootRecorder(target.IO)

	operator, err := t.checkTrainingOperator(ctx, target, recorder)
	if err != nil {
		return recorder.Build(), err
	}

	trainjobReady, err := t.checkTrainJobCRD(ctx, target, recorder)
	if err != nil {
		return recorder.Build(), err
	}

	allEntries, err := t.enumerateV1Workloads(ctx, target, recorder)
	if err != nil {
		return recorder.Build(), err
	}

	t.assessMigrationReadiness(allEntries, operator, recorder)
	t.buildSummary(allEntries, trainjobReady, operator, recorder)

	return recorder.Build(), nil
}

// trainingOperatorStatus captures the TrainingOperator component state
// collected by the trainingoperator-status step so later steps can tell an
// already-removed component apart from one that is still enabled.
type trainingOperatorStatus struct {
	state   string
	crNames []string
}

// removed reports whether TrainingOperator is fully gone from the cluster:
// not managed in the DataScienceCluster and with no leftover component CRs.
// It mirrors the warning condition (enabled or CRs present) so the two never
// disagree about what counts as removed.
func (s trainingOperatorStatus) removed() bool {
	return s.state != constants.ManagementStateManaged && len(s.crNames) == 0
}

// checkTrainingOperator warns when the TrainingOperator component is still
// enabled or its component CR still exists while upgrading to RHOAI 3.6.
// Warnings never fail the action: the user is responsible for acting on them.
func (t *verifyWorkloadsTask) checkTrainingOperator(
	ctx context.Context,
	target action.Target,
	recorder action.RootRecorder,
) (trainingOperatorStatus, error) {
	step := recorder.Child("trainingoperator-status", "Check TrainingOperator component state")

	state, err := trainingOperatorManagementState(ctx, target)
	if err != nil {
		step.Completef(result.StepFailed, "Failed to determine TrainingOperator state: %v", err)

		return trainingOperatorStatus{}, fmt.Errorf("determining TrainingOperator state: %w", err)
	}

	crNames, err := listTrainingOperatorCRNames(ctx, target)
	if err != nil {
		step.Completef(result.StepFailed, "Failed to list TrainingOperator CRs: %v", err)

		return trainingOperatorStatus{}, err
	}

	step.AddDetail("trainingoperatorState", state)
	step.AddDetail("trainingoperatorCRs", len(crNames))

	enabled := state == constants.ManagementStateManaged

	// The removal warning only applies to upgrades crossing into 3.6; earlier
	// 3.x targets still ship the (deprecated) component.
	//nolint:mnd // Version numbers 3.6
	isRemovalTarget := version.IsVersionAtLeast(target.TargetVersion, 3, 6)

	switch {
	case isRemovalTarget && enabled && len(crNames) > 0:
		step.Completef(result.StepWarning,
			"%s (state: %s, %d component CR(s) present). %s",
			msgRemovalWarning, state, len(crNames), msgRemovalAdvice)
	case isRemovalTarget && enabled:
		// Managed with no component CR is inconsistent: a healthy operator
		// keeps the CR in place while the component is managed.
		step.Completef(result.StepWarning,
			"TrainingOperator is set to Managed in DataScienceCluster but no component CR exists (inconsistent state). "+
				"This does not block the upgrade: consider setting the trainingoperator managementState to 'Removed' before upgrading")
	case isRemovalTarget && len(crNames) > 0:
		step.Completef(result.StepWarning,
			"TrainingOperator is disabled in DataScienceCluster (state: %s) but %d component CR(s) still exist (%s). This does not block the upgrade: consider cleaning them up to complete the removal",
			state, len(crNames), strings.Join(crNames, ", "))
	default:
		step.Completef(result.StepCompleted,
			"TrainingOperator state: %s, %d component CR(s) present",
			state, len(crNames))
	}

	return trainingOperatorStatus{state: state, crNames: crNames}, nil
}

// trainingOperatorManagementState reads the trainingoperator managementState
// from the DataScienceCluster singleton. A missing DSC (operator not
// installed) is reported as Removed rather than an error so the check can
// still complete on partially installed clusters.
func trainingOperatorManagementState(ctx context.Context, target action.Target) (string, error) {
	dsc, err := client.GetDataScienceCluster(ctx, target.Client)
	if err != nil {
		if client.IsResourceTypeNotFound(err) {
			return constants.ManagementStateRemoved, nil
		}

		return "", fmt.Errorf("getting DataScienceCluster: %w", err)
	}

	state, err := components.GetManagementState(dsc, constants.ComponentTrainingOperator)
	if err != nil {
		return "", fmt.Errorf("getting trainingoperator management state: %w", err)
	}

	return state, nil
}

// listTrainingOperatorCRNames lists the names of the cluster-scoped
// TrainingOperator component CRs. A missing CRD is treated as no CRs so the
// check also works on clusters that no longer serve the retired component.
func listTrainingOperatorCRNames(ctx context.Context, target action.Target) ([]string, error) {
	crType := resources.GetComponentCR(constants.ComponentTrainingOperator)
	if crType == nil {
		return nil, nil
	}

	items, err := target.Client.List(ctx, *crType)
	if err != nil {
		if client.IsResourceTypeNotFound(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("listing TrainingOperator CRs: %w", err)
	}

	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.GetName())
	}

	sort.Strings(names)

	return names, nil
}

func (t *verifyWorkloadsTask) checkTrainJobCRD(
	ctx context.Context,
	target action.Target,
	recorder action.RootRecorder,
) (bool, error) {
	step := recorder.Child("trainjob-crd", "Check TrainJob v2 CRD readiness")

	_, err := target.Client.List(ctx, resources.TrainJob)
	if err != nil {
		if client.IsResourceTypeNotFound(err) {
			step.AddDetail("trainjobCRDInstalled", false)
			step.Completef(result.StepCompleted, "TrainJob CRD not installed — v2 API not yet available on this cluster")

			return false, nil
		}

		step.AddDetail("trainjobCRDInstalled", false)
		step.Completef(result.StepFailed, "Failed to check TrainJob CRD: %v", err)

		return false, fmt.Errorf("checking TrainJob CRD: %w", err)
	}

	step.AddDetail("trainjobCRDInstalled", true)
	step.Completef(result.StepCompleted, "TrainJob CRD installed — v2 API available")

	return true, nil
}

func (t *verifyWorkloadsTask) enumerateV1Workloads(
	ctx context.Context,
	target action.Target,
	recorder action.RootRecorder,
) ([]WorkloadEntry, error) {
	entries, failures, err := EnumerateWorkloads(ctx, target.Client)

	failuresByKind := make(map[string]ListFailure, len(failures))
	for _, f := range failures {
		failuresByKind[f.Kind] = f
	}

	entriesByKind := make(map[string][]WorkloadEntry)
	for _, e := range entries {
		entriesByKind[e.Kind] = append(entriesByKind[e.Kind], e)
	}

	for _, rt := range TrainingJobTypes {
		step := recorder.Child(rt.Resource, fmt.Sprintf("List %s workloads", rt.Kind))

		if f, ok := failuresByKind[rt.Kind]; ok {
			step.Completef(result.StepFailed, "Failed to list: %v", f.Err)

			continue
		}

		kindEntries := entriesByKind[rt.Kind]
		if len(kindEntries) == 0 {
			step.Completef(result.StepCompleted, "No %s found", rt.Kind)

			continue
		}

		for _, entry := range kindEntries {
			step.Recordf(
				entry.Name,
				"%s/%s — %s (age: %s)",
				result.StepCompleted,
				entry.Namespace, entry.Name, entry.Status, entry.Age,
			)
		}

		step.Completef(result.StepCompleted, "Found %d %s(s)", len(kindEntries), rt.Kind)
	}

	if err != nil {
		return entries, fmt.Errorf("enumerating v1 workloads: %w", err)
	}

	return entries, nil
}

// assessMigrationReadiness reports active v1 jobs as warnings. Active jobs
// stop being reconciled once TrainingOperator is no longer managed, so users
// are advised to let them complete or delete them first — the upgrade itself
// is never blocked. Job activity alone cannot tell whether the operator was
// already removed (completed jobs are normal on a managed cluster too), so
// the "already removed" wording is gated on the actual component state.
func (t *verifyWorkloadsTask) assessMigrationReadiness(
	entries []WorkloadEntry,
	operator trainingOperatorStatus,
	recorder action.RootRecorder,
) {
	step := recorder.Child("migration-readiness", "Assess migration readiness")

	var active []WorkloadEntry

	for _, e := range entries {
		if isActiveStatus(e.Status) {
			active = append(active, e)
		}
	}

	step.AddDetail("active", len(active))
	step.AddDetail("completed", len(entries)-len(active))

	switch {
	case len(entries) == 0:
		step.Completef(result.StepCompleted, "No v1 training workloads found — nothing to migrate")
	case len(active) == 0 && operator.removed():
		step.Completef(result.StepCompleted, "TrainingOperator has been removed - nothing to migrate")
	case len(active) == 0:
		step.Completef(result.StepCompleted, "No active v1 jobs — nothing to migrate")
	default:
		for _, a := range active {
			step.Recordf(
				a.Name,
				"%s/%s is %s — consider letting it complete or deleting it before upgrading",
				result.StepWarning,
				a.Namespace, a.Name, a.Status,
			)
		}

		step.Completef(result.StepWarning,
			"Found %d active v1 job(s) — active workloads stop being reconciled once TrainingOperator is no longer managed. This does not block the upgrade: consider letting them complete or deleting them first",
			len(active))
	}
}

func (t *verifyWorkloadsTask) buildSummary(
	entries []WorkloadEntry,
	trainjobReady bool,
	operator trainingOperatorStatus,
	recorder action.RootRecorder,
) {
	step := recorder.Child("summary", "Migration summary")

	report := BuildReport(entries)

	step.AddDetail("total", report.Summary.Total)
	step.AddDetail("byKind", report.Summary.ByKind)
	step.AddDetail("byStatus", report.Summary.ByStatus)
	step.AddDetail("trainjobCRDInstalled", trainjobReady)

	migrationMap := buildMigrationMap(report.Summary.ByKind)
	step.AddDetail("migrationMap", migrationMap)

	var activeCount int

	for _, e := range entries {
		if isActiveStatus(e.Status) {
			activeCount++
		}
	}

	step.AddDetail("active", activeCount)
	step.AddDetail("completed", report.Summary.Total-activeCount)

	if activeCount > 0 {
		step.Completef(result.StepWarning,
			"Found %d v1 training workload(s): %d active, %d completed — consider draining the active ones and migrating to Trainer v2 TrainJob",
			report.Summary.Total, activeCount, report.Summary.Total-activeCount)

		return
	}

	switch {
	case report.Summary.Total == 0:
		step.Completef(result.StepCompleted, "No v1 training workloads found — nothing to migrate")
	case operator.removed():
		step.Completef(result.StepCompleted,
			"Found %d v1 training workload(s), none active — TrainingOperator has been removed and these workloads are no longer managed",
			report.Summary.Total)
	default:
		step.Completef(result.StepCompleted,
			"Found %d v1 training workload(s), none active — nothing to migrate",
			report.Summary.Total)
	}
}

func isActiveStatus(status string) bool {
	lower := strings.ToLower(status)

	return lower == "running" || lower == "created"
}

func buildMigrationMap(byKind map[string]int) map[string]string {
	migrationMap := make(map[string]string)

	for kind := range byKind {
		if runtime, ok := v1KindToV2Runtime[kind]; ok {
			migrationMap[kind] = fmt.Sprintf("TrainJob with runtime %q", runtime)
		}
	}

	return migrationMap
}
