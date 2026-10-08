package training

const (
	verifyWorkloadsID          = "training.verify-workloads"
	verifyWorkloadsName        = "Verify training workloads"
	verifyWorkloadsDescription = "Pre-upgrade check for Kubeflow v1 training workloads and TrainingOperator removal in RHOAI 3.6 (advisory only, does not block the upgrade)"
)

// TrainingOperator removal warning shown when upgrading to RHOAI 3.6: the
// component is removed from the product and from DataScienceCluster v3, so
// anything left enabled stops being managed by the operator.
const (
	msgRemovalWarning = "TrainingOperator (Kubeflow Training Operator v1) is removed in RHOAI 3.6. " +
		"If you keep it enabled, it will no longer be managed after the upgrade " +
		"and you will be responsible for removing it manually"

	msgRemovalAdvice = "This does not block the upgrade: consider setting the trainingoperator managementState to " +
		"'Removed' in your current version - this cleans up everything - then use the new Trainer operator in RHOAI 3.6"
)

//nolint:gochecknoglobals // Immutable mapping from v1 training job kinds to v2 TrainJob runtimes
var v1KindToV2Runtime = map[string]string{
	"PyTorchJob": "torch",
	"TFJob":      "tensorflow",
	"MPIJob":     "mpi",
	"XGBoostJob": "xgboost",
}
