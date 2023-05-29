# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-RC3] - 2023-05-xx

### Added
- Option to add a resource `GET info/{uuid}` which returns more information about a job. Can be configured via `ApiBuilder.infoConfig` 

### Changed
- It is now verified on all resources that the UUID really belongs to the respective job type. Such requests will be answered with HTTP code 400 and a respective error message.
- Detect and prevent the definition of multiple APIs or jobs with the same job type. The second API/job with the same job type will cause an `IllegalArgumentException` to be thrown.
- The main method `JobFramework` was split into two methods to simplify the API: Instead of taking an `Either<Application, CoroutineScope>`, there is now one method taking an `Application` and one taking a `CoroutineScope`. 
- Renamed method `fetchStati` of `JobPersistence` to `fetchStates`


## [1.0.0-RC2] - 2023-05-08

### Added
- Option to add a `delete` resource to an api which deletes the job, its input, result, and possible failure from the persistence. Can be configured via `ApiConfigBuilder.enableDeletion`, default is `false`.
- Option to restart all `RUNNING` jobs of an instance on startup. This is enabled by default and highly recommended, it can however be deactivated using `MaintenanceConfig.restartRunningJobsOnStartup`.
- Option to create the framework in testing mode using `JobFrameworkTestingMode`. The result is an object of type `JobFrameworkTestingApi` which allows to manually submit jobs, run the executor or run any other maintenance job.
- Hierarchical jobs which can be set up via `JobFrameworkBuilder.addApiForHierarchicalJob`
- Option to specify the maximum restart per job via `JobConfig.maxRestarts`.
- Marked `JobFramework` builder classes as DSL
- Option to configure the path to the synchronous resource via `SynchronousResourceConfig.path`
- HashMap implementation of the persistence layer for testing purposes (`HashMapJobPersistence` and `HashMapDataPersistence`)

### Changed
- Changed key names in `DefaultRedisConfig`
- `MainJobExecutor` and `SpecificExecutor` are now public
- The computation of a job must now return a `ComputationResult` which is either `ComputationResult.Success` or `ComputationResult.Error`. A result successful computation will be stored with status `SUCCESS`, in case of an error the job may be restarted depending on `ComputationResult.Error.tryRepeat` (provided that `JobConfig.maxRestarts` has not been reached yet).
- Moved `Persistence` to `api.persistence` and Redis persistence implementation to `api.persistence.redis`

### Removed
- `MaintenanceConfig.maxJobRestarts` in favor for a configuration on job basis (see above)

## [1.0.0-RC1] - 2023-04-13

### Added
- Initial version of the Kotlin Job Framework
