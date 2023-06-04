# KJobs -- A Job Framework for Asynchronous Webservices in Kotlin and Ktor

This readme is work in progress.

## [REMOVE] Remaining Tests
- Pure Jobs
- Some more errors (check coverage)

## Getting Started

Checkout the documentation at www.kjobs.org

## Further ideas
- Route documentation with https://github.com/SMILEY4/ktor-swagger-ui
- Split the application into separate modules (`core`, `redis`, `ktor`, `ktor-swagger`,...)
- Allow computations to announce their progress
  - additional property `Job.progress` as a random string value
  - additional resource `GET progress/{uuid}` returning
    - `Job.progress` if the job is in status `RUNNING` and `progress` is not `null`
    - `Job.status` otherwise
  - callback for computations allowing to submit the progress
- Allow configurable resource names instead of hardcoded ones (`submit`, `status`, etc)
- Allow multiple jobs to be started when checking for new jobs (right now, only one job per `jobCheckInterval` can be started)
- Generify Hierarchical Jobs s.t. they become the standard. They can optionally get `HierarchicalJobApi`s (for *any* configured job, so no need anymore to `addDependentJob`s to hierarchical jobs) and thus maybe even submit other hierarchical jobs.
