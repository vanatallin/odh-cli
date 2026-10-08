package trainingoperator

import (
	"context"
	"fmt"

	"github.com/opendatahub-io/odh-cli/pkg/constants"
	"github.com/opendatahub-io/odh-cli/pkg/lint/check"
	"github.com/opendatahub-io/odh-cli/pkg/lint/check/result"
	"github.com/opendatahub-io/odh-cli/pkg/lint/check/validate"
	"github.com/opendatahub-io/odh-cli/pkg/util/client"
	"github.com/opendatahub-io/odh-cli/pkg/util/components"
	"github.com/opendatahub-io/odh-cli/pkg/util/version"
)

// RemovalCheck warns that TrainingOperator v1 is removed in RHOAI 3.6 when the
// component is still enabled. The finding is advisory: the upgrade can proceed,
// but the component stops being managed afterwards.
type RemovalCheck struct {
	check.BaseCheck
}

func NewRemovalCheck() *RemovalCheck {
	return &RemovalCheck{
		BaseCheck: check.BaseCheck{
			CheckGroup:       check.GroupComponent,
			Kind:             constants.ComponentTrainingOperator,
			Type:             check.CheckTypeRemoval,
			CheckID:          "components.trainingoperator.removal",
			CheckName:        "Components :: TrainingOperator :: Removal (3.6)",
			CheckDescription: "Warns that TrainingOperator (Kubeflow Training Operator v1) is removed in RHOAI 3.6 - if kept enabled it will no longer be managed after the upgrade and must be removed manually",
			CheckRemediation: "Set trainingoperator managementState to 'Removed' in your current version - this cleans up everything - then use the Trainer operator (Trainer v2) in RHOAI 3.6. If kept enabled, the component will no longer be managed after the upgrade and you will be responsible for removing it manually.",
		},
	}
}

// CanApply returns whether this check should run for the given target.
// Applies when target version >= 3.6 and TrainingOperator is Managed.
func (c *RemovalCheck) CanApply(ctx context.Context, target check.Target) (bool, error) {
	//nolint:mnd // Version numbers 3.6
	if !version.IsVersionAtLeast(target.TargetVersion, 3, 6) {
		return false, nil
	}

	dsc, err := client.GetDataScienceCluster(ctx, target.Client)
	if err != nil {
		return false, fmt.Errorf("getting DataScienceCluster: %w", err)
	}

	return components.HasManagementState(dsc, constants.ComponentTrainingOperator, constants.ManagementStateManaged), nil
}

func (c *RemovalCheck) Validate(ctx context.Context, target check.Target) (*result.DiagnosticResult, error) {
	return validate.Component(c, target).
		Run(ctx, validate.Removal("TrainingOperator (Kubeflow v1) is enabled (state: %s) but is removed in RHOAI %s - "+
			"it will no longer be managed after the upgrade and you will be responsible for removing it manually. Use Trainer v2 instead.",
			check.WithImpact(result.ImpactAdvisory),
			check.WithRemediation(c.CheckRemediation)))
}
