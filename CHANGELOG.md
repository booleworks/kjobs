# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-RC2] - 2023-04-xx

### Added
- Option to restart all `RUNNING` jobs of an instance on startup. This is enabled by default and highly recommended, it can however be deactivated using `MaintenanceConfig.restartRunningJobsOnStartup`.
- Option to create the framework in testing mode using `JobFrameworkTestingMode`. The result is an object of type `JobFrameworkTestingApi` which allows to manually submit jobs, run the executor or run any other maintenance job. 

### Changed
- Changed key names in `DefaultRedisConfig`
- `MainJobExecutor` and `SpecificExecutor` are now public


## [1.0.0-RC1] - 2023-04-13

### Added
- Initial version of the Kotlin Job Framework
