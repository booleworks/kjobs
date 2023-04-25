# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-RC2] - 2023-04-xx

### Added
- Option to restart all `RUNNING` jobs of an instance on startup. This is enabled by default and highly recommended, it can however be deactivated using `MaintenanceConfig.restartRunningJobsOnStartup`.
- Option to create the framework in testing mode using `JobFrameworkTestingMode`. The result is an object of type `JobFrameworkTestingApi` which allows to manually submit jobs, run the executor or run any other maintenance job.
- Option to specify the maximum restart per job via `JobConfig.maxRestarts`.
- Marked `JobFramework` builder classes as DSL
- Option to configure the path to the synchronous resource via `SynchronousResourceConfig.path` 

### Changed
- Changed key names in `DefaultRedisConfig`
- `MainJobExecutor` and `SpecificExecutor` are now public
- The computation of a job must now return a `ComputationResult` which is either `ComputationResult.Success` or `ComputationResult.Error`. A result successful computation will be stored with status `SUCCESS`, in case of an error the job may be restarted depending on `ComputationResult.Error.tryRepeat` (provided that `JobConfig.maxRestarts` has not been reached yet).

### Removed
- `MaintenanceConfig.maxJobRestarts` in favor for a configuration on job basis (see above)

## [1.0.0-RC1] - 2023-04-13

### Added
- Initial version of the Kotlin Job Framework
