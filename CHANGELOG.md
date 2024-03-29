# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-RC9] - 2023-12-12

### Changed
- Fields of open persistence classes are now protected to allow proper inheritance
- Significantly improved performance of fetching jobs Redis implementation (by using [Pipelining](https://redis.io/docs/manual/pipelining/))
- Some dependency updates, especially SLF4J from 1.7.36 to 2.0.9


## [1.0.0-RC8] - 2023-11-13

### Changed
- Bugfix in `RedisJobTransactionalPersistence.updateJob`: `HDEL` throws an error if no fields are given, so it should only be called if at least one field is `null`.


## [1.0.0-RC7] - 2023-10-27

### Changed
- Bugfix for `scheduleForever`: Exceptions from the task will not be propagated up anymore and cause the whole scheduling to be aborted. Instead, they are just ignored (and usually logged to the console).
- Removed the internal feature again that the heartbeat also checks for the main executor to run. This should be superfluous with the above bugfix.


## [1.0.0-RC6] - 2023-10-26

### Added
- `JobFramework()` method now returns a coroutine job which can be used to terminate all maintenance jobs
- A new statistics resource which can be enabled in a `JobFrameworkBuilder` via `enableStatistics`. By default (if enabled), it creates a resource `GET statistics` which returns a `JobStatistics` object in JSON format.
- A new method `JobPersistence.fetchAllJobs` which must be implemented by the user of the library (unless the `RedisJobPersistence` is used where it is already implemented).
- The heartbeat now also checks if the executor job is still running regularly. If it is not, the heartbeat will not be updated anymore, s.t. the instance may be considered dead.

### Changed
- Renamed `cancellationConfig` to `enableCancellation`. Setting `enabled = true` is not necessary/possible anymore.
- `DataPersistence.dataTransaction` now takes a type parameter `T` s.t. the result of the `block` can be returned
- Additional safety net in case of database connection problems (KJobs will repeatedly try to set the job to `FAILURE` to avoid that the job remains in state `RUNNING` although it failed because of the database problems)
- Updated to Kotlin 1.9.10, Redis to 5.0.2, and some other minor dependency updates
- Some minor internal refactoring


## [1.0.0-RC5] - 2023-06-16

### Added
- New configuration properties to customize the configuration of routes. This can be used to simply changing the path of a resource, but also to use different route creation commands which can be required e.g. to generate OpenAPI definitions. The respective properties are defined on `ApiConfigBuilder` and are initialized with the current default behavior. The following properties were added:
  - `submitRoute` with default `{ block -> post("submit") { block() } }`
  - `statusRoute` with default `{ block -> get("status/{uuid}") { block() } }`
  - `resultRoute` with default `{ block -> get("result/{uuid}") { block() } }`
  - `failureRoute` with default `{ block -> get("failure/{uuid}") { block() } }`
  - `deleteRoute` with default `{ block -> delete("delete/{uuid}") { block() } }`
  - `cancelRoute` with default `{ block -> post("cancel/{uuid}") { block() } }`
  - `syncRoute` with default `{ block -> post("synchronous") { block() } }`
  - `infoRoute` with default `{ block -> get("info/{uuid}") { block() } }`

### Removed
- `ApiConfigBuilder.basePath` was removed, since it can trivially be implemented from outside (just surround the call to `addApi(...)` with `route("basePath") {...}`)
- `SynchronousResourceConfigBuilder.path` was removed -- use `ApiConfigBuilder.syncRoute` instead (`syncRoute = { block -> post("path") { block() } }`)
- `JobInfoConfigBuilder.path` was removed -- use `ApiConfigBuilder.jobInfoRoute` instead (`jobInfoRoute = { block -> get("jobInfo") { block() } }`)

### Changed
- Minor dependency updates

## [1.0.0-RC4] - 2023-06-07

### Changed
- Bugfix: Task scheduling in `scheduleForever` was dispatched to the default dispatcher instead of the dispatcher from the context.


## [1.0.0-RC3] - 2023-06-06

### Added
- Option to add a resource `GET info/{uuid}` which returns more information about a job. Can be configured via `ApiBuilder.infoConfig`

### Changed
- Scheduling of maintenance tasks was adjusted. We now create a new thread pool for scheduling the tasks. The `CoroutineDispatcher` for the computation/executor task can be configured via the new parameter `ExecutorConfig.dispatcher`, all other tasks (e.g. updating heartbeats, looking for old jobs or dead instances, etc.) will be executed using `Dispatchers.IO`. According to these changes there are two new configuration options:
  - `MaintenanceConfig.threadPoolSize` determining the number of threads to be used for task scheduling, the default is 2.
  - `ExecutorConfig.dispatcher` which allows to determine which `CoroutineDispatcher` should be used for all computations, the default is `Dispatchers.Default`.
- The main method `JobFramework` dropped the parameter `Either<Application, CoroutineScope>`, since it is not required anymore with the adjusted task scheduling.
- It is now verified on all resources that the UUID really belongs to the respective job type. Such requests will be answered with HTTP code 400 and a respective error message.
- Detect and prevent the definition of multiple APIs or jobs with the same job type. The second API/job with the same job type will cause an `IllegalArgumentException` to be thrown.
- Renamed two methods of `JobPersistence`:
  - `fetchStati` to `fetchStates`
  - `fetchHeartBeats` to `fetchHeartbeats` (for consistency with `JobTransactionalPersistence.updateHeartbeat`)


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
