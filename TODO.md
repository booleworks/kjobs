## TODO
- Rename `JobPrioritizer` to `JobSelector`?
- Change artifact name/group in order to allow more flexible artifact names later on?

## Further ideas
- Split the application into separate modules (`core`, `redis`, `ktor`, `ktor-swagger`,...)
- Statistics route with information about all jobs (by type -- e.g. how many jobs are in which state)
- Allow cancellation to be configured on API level instead of framework level
- Combine the configuration of additional resources (`infoConfig`, `synchronousResourceConfig` etc) in a `additionalResources`-Builder
- Allow computations to announce their progress
    - additional property `Job.progress` as a random string value
    - additional resource `GET progress/{uuid}` returning
        - `Job.progress` if the job is in status `RUNNING` and `progress` is not `null`
        - `Job.status` otherwise
    - callback for computations allowing to submit the progress
- Maintenance job to clear obsolete (too old) heartbeats
- Allow configurable resource names instead of hardcoded ones (`submit`, `status`, etc)
- Allow multiple jobs to be started when checking for new jobs (right now, only one job per `jobCheckInterval` can be started)
- Generify Hierarchical Jobs s.t. they become the standard. They can optionally get `HierarchicalJobApi`s (for *any* configured job, so no need anymore to `addDependentJob`s to hierarchical jobs) and thus maybe even submit other hierarchical jobs.

## Remarks to release 1.1.0 regarding timeouts
- The `JobConfigBuilder.timeoutComputation` provides a timeout for the computation time of a job. When a job is started `timeoutComputation` is called to determine the time available time to perform the computation. If the computation of a job is not finished within this timeout, the computation is cancelled and the job is set to an error result with message `The job did not finish within the configured timeout of $timeout`.
- The `JobConfigBuilder.timeoutComputation` does *not* include the waiting time until the job is picked up by a free instance, it is solely a timeout for the computation time.
- In some situations it can be required to specify a timeout for the total time (waiting time + computation time). E.g., a caller that wants to wait x seconds in total for a job to be finished after the job was submitted. In this case, the caller expects the job to be finished in x seconds, independent of how much time the job waited until it was started.
- An implementation of two different timeout types (computation timeout and total timeout) requires larger changes in Kjobs, including an extension of the data base model for jobs. To avoid larger changes in the code base we opted for a different approach in release 1.1.0 to be able to set a total timeout:
    - `JobConfigBuilder.timeoutComputation` is now explicitly allowed to return non-positive values. With that change the `timeoutComputation` can be specified to only contain the remaining time after the already passed waiting time is substracted. The remaining time might be a non-positive value if the waiting time was already too long. In this case the job is immediately set to a `CANCELLED` state when picked up.
    - `SynchronousResourceConfigBuilder.maxWaitingTime` changed its type from `Duration` to a function from `Job` and `INPUT` to `Duration` (i.e. `(Job, INPUT) -> Duration`). This allows to specify a timeout depending on a job and/or input and also to set the maximal waiting time for the sync endpoint to only the remaining time after the already passed waiting time is substracted. Otherwise, the sync endpoint would wait `maxWaitingTIme` even if the job's timeout was already exceeded.
- There is a minor downside to this approach we deliberately accepted: If the computation of a job got cancelled due to the computation time. The message in the error result states `The job did not finish within the configured timeout of $timeout` with the timeout of the computation time and not with the total time. This might look like the total timeout was not used and can cause confusion.
