# KJobs -- A Job Framework for Asynchronous Webservices in Kotlin and Ktor

This readme is work in progress.

## Getting Started

The starting point for creating new APIs is `com.booleworks.kjobs.api.JobFramework()`.

## Documentation (Outline)

### General Setup
- Set up one or more APIs with `com.booleworks.kjobs.api.JobFramework()`
  - generated resources (`submit`, `status`, `result`, `failure`)
- Persistence

### Features
- Configure selection of jobs
  - Tags
  - Priorities
  - Execution Capacity of Nodes
  - Custom Info
- Errors, Timeouts, Restarts
- Additional resources
  - Sync Mock
  - Cancel Jobs
  - Delete Jobs
  - Job Info
- Maintenance
- Jobs without API
- Hierarchical Jobs
- Testing API
- Provided persistence implementations
  - Redis
  - HashMap (testing/one-node-deploy only)

### How does it work internally?

### Reference of all Configuration Options

## Further ideas
- Route documentation with https://github.com/SMILEY4/ktor-swagger-ui
- Move Redis implementation to separate module/package
- Make Ktor optional (and move the support for Ktor to a separate module/package)
- Allow computations to announce their progress
  - additional property `Job.progress` in `[0..100]` (or better: a random string value)
  - additional resource `progress` returning the progress or some value for "unknown"
  - callback for computations allowing to submit the progress
- Split the application into separate modules (`core`, `redis`, `ktor`, `ktor-swagger`,...)
- Allow configurable resource names instead of hardcoded ones (`submit`, `status`, etc)
- Allow multiple jobs to be started when checking for new jobs (right now, only one job per `jobCheckInterval` can be started)
- Generify Hierarchical Jobs s.t. they become the standard. They can optionally get `HierarchicalJobApi`s (for *any* configured job, so no need anymore to `addDependentJob`s to hierarchical jobs) and thus maybe even submit other hierarchical jobs.
