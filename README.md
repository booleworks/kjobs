# KJobs -- A Job Framework for Asynchronous Webservices in Kotlin and Ktor

This readme is work in progress.

## Getting Started

The starting point for creating new APIs is `com.booleworks.kjobs.api.JobFramework()`.


## Further ideas
- Route documentation with https://github.com/SMILEY4/ktor-swagger-ui
- Make Ktor optional
- Split the application into separate modules (`core`, `redis`, `ktor`, `ktor-swagger`,...)
- Allow configurable resource names instead of hardcoded ones (`submit`, `status`, etc)
- Allow multiple jobs to be started when checking for new jobs (right now, only one job per `jobCheckInterval` can be started)
- Generify Hierarchical Jobs s.t. they become the standard. They can optionally get `HierarchicalJobApi`s (for *any* configured job, so no need anymore to `addDependentJob`s to hierarchical jobs) and thus maybe even submit other hierarchical jobs.
