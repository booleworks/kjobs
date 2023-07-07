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
