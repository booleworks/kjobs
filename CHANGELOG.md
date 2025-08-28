# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-RC24] - 2025-08-xx

### Changed
- Improved `RedisLongPollManager` performance by using a single Redis connection for publishing a completed job instead of creating a new one each time.
- Improved `RedisPersistence` performance by re-using a couple of Redis connections instead of creating a new one each time.

### Added
- Configuration parameter `RedisConfig.scanLimit` to configure the limit of the Redis scan operations. The default is 1000 and has the same value as the previously hardcoded limit.

## [1.0.0-RC23] - 2025-05-07

### Changed
- Main job execution tasks (heartbeat update, capacity check, and launching the new computation) now run on `Dispatchers.IO`
- Renamed `ExecutorConfig.dispatcher` to `computationDispatcher`
- Minor dependency updates

### Fixed
- Job execution, previously run on `Dispatchers.Default`, now correctly uses the configured `ExecutorConfig.computationDispatcher`
- Heartbeat updates could be blocked by heavy jobs because main execution ran on the custom dispatcher (per default `Dispatchers.Default`)

## [1.0.0-RC22] - 2025-04-11

### Changed
- Additional to the function where jobs older than a given timespan are deleted automatically, it is now possible to define a maximum number of allowed jobs. If the overall count of finished jobs exceeds the maximum number of allowed jobs defined in `MaintenanceConfig.deleteOldJobsOnExceedingCount`, the oldest jobs regarding their creation date are deleted. This function counts all jobs disgarding their status, but deletes only the oldest finished jobs. 
- Following example showcases the above behaviour: Assuming the DB contains 10 jobs of which 2 are in status RUNNING and all others are in status SUCCESSFUL and `MaintenanceConfig.deleteOldJobsOnExceedingCount` is set to 8. Then there are `10 - 8 = 2` jobs over the defined maximum in the db. Thus the two jobs with the earliest creation date are deleted.  
- Due to the new method `Maintenance#deleteOldJobsExceedingDbJobCount` (for details see below Added section) the existing `Maintenance#deleteOldJobs` is renamed to `Maintenance#deleteOldJobsFinishedBefore`
- Minor dependency updates

### Added
- Parameter `MaintenanceConfig.deleteOldJobsOnExceedingCount` as described above
- Method `Maintenance#deleteOldJobsExceedingDbJobCount` which deletes the oldest jobs, sorted by creation date, if the overall count of finished jobs exceeds the maximum allowed job count defined in `MaintenanceConfig.deleteOldJobsOnExceedingCount`  
- New scheduled function for deleting jobs that exceed the defined maximum job count as described above. The execution is only triggered when maintenance in the job framework is activated. The execution interval is defined in `MaintenanceConfig.oldJobDeletionInterval`. Both scheduled deletion functions are executed (the deletion on exceeding a maximum job count and the deletion of jobs that are older then a defined time span), means they are not mutually exclusive.   

## [1.0.0-RC21] - 2025-01-30

### Changed
- `ApiConfigBuilder.inputValidation` must now return a new `InputValidationResult` which allows to configure a single error message and a return code instead of code 400

## [1.0.0-RC20] - 2024-09-18

### Changed
- The heartbeat is now updated during the main run of the `JobExecutor`, i.e. every time the instance checks if new jobs are available. This makes the heartbeat more "reliable", because if the `JobExecutor` has crashed (which should usually never happen, but does nevertheless in unfortunate circumstances) the instance is effectively useless. For this reason, the parameter `MaintenanceConfig.heartbeatInterval` was removed (since it is now the same as `jobCheckInterval`) and the new parameter `MaintenanceConfig.heartbeatTimeout` was added, which describes after which time after the last heartbeat update the instance should be considered broken/dead.
- Minor dependency updates

### Added
- Parameter `MaintenanceConfig.heartbeatTimeout` as described above
- New method `Maintenance.livenessCheck` which checks if a given instance is alive. This can be useful for liveness probes in cloud environments.

### Removed
- Parameter `MaintenanceConfig.heartbeatInterval` as described above


## [1.0.0-RC19] - 2024-07-11

### Changed
- Prefixed all logger names with `com.booleworks.kjobs.`
- Internal API: `JobExecutor.execute()` now starts the computation independently (not as a child coroutine) and returns the coroutine job of the computation
- Minor dependency updates

### Removed
- Parameter `MaintenanceConfig.preventParallelExecutionOfBackgroundJobs`: From now on, background jobs (except for the update of heartbeats) will never be executed in parallel, since this may lead to inconsistencies


## [1.0.0-RC18] - 2024-06-21

### Changed
- Improved Redis implementation by using `SCAN` instead of the dangerous `KEYS` command


## [1.0.0-RC17] - 2024-06-13

### Added
- New configuration parameter `MaintenanceConfig.preventParallelExecutionOfBackgroundJobs`: By default, background jobs (especially the check for new jobs) are started independently of the last run of the job. So e.g. if the `jobCheckInterval` is 1 second and the actual check takes 2 seconds, there will always be two checks running at the same time. Usually the job check should be fast, but depending on the job load and persistence implementation it may be useful to prevent parallel executions. So if this parameter is set to `true`, the next run will not be triggered when the previous run was not finished. This setting applies to all *background* jobs *except* for the heartbeat update.

### Changed
- Another bugfix in `transactionWithPreconditions` implementation for Redis


## [1.0.0-RC16] - 2024-06-11

### Changed
- Bugfix in `transactionWithPreconditions` implementation for Redis


## [1.0.0-RC15] - 2024-06-10

### Added
- New method `JobPersistence.transactionWithPreconditions` which executes a transaction only if a given set of preconditions is still met when the transaction is executed. This should improve the reservation and cancellation of jobs.

### Changed
- Minor bug fixes and test improvements
- Moved to Kotlin 2.0.0 and Java 17
- Minor dependency updates


## [1.0.0-RC14] - 2024-05-08

### Changed
- Minor dependency updates to fix vulnerabilities.


## [1.0.0-RC13] - 2024-02-02

### Changed
- Fixed a connection leak in the Redis persistence implementation caused by the transition to Lettuce.


## [1.0.0-RC12] - 2024-01-24

### Changed
- Workaround for Redis Persistence until [this Lettuce issue](https://github.com/lettuce-io/lettuce-core/issues/2608) is answered or solved. Problem is that we cannot store the job and its input in Redis within one transaction using Lettuce. Until now, the input was stored after the job itself, s.t. other instances/threads found the job but no input. The workaround is that we store it the other way round (the input will not be discovered by other instances/threads without the job).


## [1.0.0-RC11] - 2024-01-17

### Changed
- Bugfix: Transactions for `RedisDataTransaction` did not work properly in some cases


## [1.0.0-RC10] - 2024-01-16

### Added
- Support for **Long Polling** which is an alternative to `status` calls. Instead of returning the current status directly (as for `status` calls), a call to `poll/<uuid>` will wait for the job to finish or until a configured timeout is hit. This can significantly reduce the number of (usually `status`) calls coming from clients. Furthermore, the clients don't need to think about reasonable intervals for `status` calls anymore.
  - The result of a long polling call is a `PollStatus` which can be `SUCCESS`, `FAILURE`, `ABORTED` (if the job was already cancelled), or `TIMEOUT`.
  - Long polling must be enabled via `ApiBuilder.enableLongPolling` and can be configured with a `LongPollManager` and a timeout. It is recommended to set the timeout to at most 10 minutes or less to avoid read timeouts of client or server.
  - Clients are free (and encouraged) to start the next poll call immediately after receiving a `TIMEOUT` response 
  - Long polling requires a `LongPollManager` which may be tricky when running on multiple instances. However, a Redis-based implementation is already provided by KJobs.
  - Clients can request a specific timeout in milliseconds by passing the query parameter `timeout`, e.g. `poll/<uuid>?timeout=1000` will wait at least one second for the result. The timeout is ignored if it is larger than the one configured in `LongPollingConfigBuilder.maximumConnectionTimeout`.
- Support for automatic (GZIP) compression of data stored in Redis via `RedisConfig.useCompression`. If enabled, the `INPUT` and `RESULT` are transparently compressed when read/written to Redis by  `RedisDataPersistence` and `RedisDataTransactionalPersistence`.


### Changed
- Using [Lettuce](https://lettuce.io/) instead of [Jedis](https://github.com/redis/jedis) for the Redis-based persistence implementation, thus `RedisJobPersistence` and `RedisDataPersistence` now have to be initialized with a `RedisClient` instead of a `JedisPool`. You can create a `RedisClient` like this: `RedisClient.create(RedisURI(host, bindPort, 1.minutes.toJavaDuration()))`
- Internal refactoring of job cancellation queue (eliminated global state in `Maintenance.jobToBeCancelled` with an atomic reference which is passed as parameter)
- Added default empty configuration for `JobFrameworkBuilder.enableCancellation` and `JobFrameworkBuilder.enableStatistics`
- Renamed `ApiBuilder.synchronousResourceConfig` to `enableSynchronousResource` which enables the synchronous resource on the method call, thus `SynchronousResourceConfigBuilder.enabled` cannot be set anymore from outside
- Renamed `ApiBuilder.infoConfig` to `enableJobInfoResource` which enables the job info resource on the method call, thus `JobInfoConfigBuilder.enabled` cannot be set anymore from outside


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
- Added names to all created coroutines, thus the method `scheduleForever` now also requires an (arbitrary) coroutine name
- Updated to Kotlin 1.9.22, Redis to 5.0.2, and some other minor dependency updates
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
