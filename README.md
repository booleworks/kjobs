# KJobs - A Job Framework for Asynchronous Web Services in Kotlin and Ktor

[![Maven Central](https://img.shields.io/maven-central/v/com.booleworks/kjobs.svg?label=Maven%20Central)](https://mvnrepository.com/artifact/com.booleworks/kjobs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Kotlin](https://img.shields.io/badge/kotlin-1.9.22-blue.svg?logo=kotlin)](http://kotlinlang.org)

## Introduction

There are many projects we at BooleWorks have been working on, where we faced the same simple situation:

We're creating a web service which is performing some computationally heavy job(s). Depending on the input such jobs may only take a second, others may take several minutes or in rare cases even hours. The natural solution today is to use an asynchronous REST service where the caller submits a job and receives some kind of ID. With this ID the caller can then check the status of the job and finally retrieve the result of the computation. However, there are some pitfalls which are not trivial to circumvent, especially in today's cloud-based deployments:
- we can have multiple instances of our application running on different pods, and we don't want the incoming jobs to be distributed randomly (e.g. via round-robin) among the pods, but rather distribute the workload evenly. Otherwise, one instance might get all the computationally heavy jobs which all take several minutes to compute, while another gets the same number of jobs which are all finished after some seconds. Instead, it might be more useful if an instance could *select* its next job based on some criteria.
- in a cloud environment restarts of pods can happen any time, and we want to make sure that jobs being computed by such a pod are not "forgotten"
- we do not want complicate deployment and operation by distinguishing between supervisor and runner nodes

KJobs aims to solve these kind of problems and to provide a comfortable way to set up such a web service.

## Overview

### Main Features

- Asynchronous
- No supervisor or broker, every instance/pod is fully self-contained
- Internal load balancing: Submitted jobs will be computed by the first instance which has enough "capacity"
- REST resources for each API are automatically generated (but can be customized)
- Jobs are persisted together with input and output
- Persistence layer is fully generic, use the provided Redis implementation or implement your own persistence for persistence solutions (like a classical or in-memory database)
- Heartbeats -- dead instances are identified and their jobs are automatically restarted by another instance
- Optional additional resources like synchronous API (which internally calls the asynchronous API), cancellation of jobs, and some more
- Possibility to document routes with OpenAPI/Swagger
- Beta: Definition of dependent jobs simplifies the parallelization of large computations

### Further Features

- Job Execution
  - Jobs can programmatically receive tags and instances can be configured to compute only jobs with specific tags
  - Custom prioritization of jobs (default is FIFO)
  - Execution capacity of instances can be defined (e.g. to prevent that an instance computes two possibly long-running jobs in parallel)
  - Timeouts for computations
  - Optional restarts of failed jobs
- Maintenance
  - Old jobs can be deleted after a given time
  - Intervals of all background jobs (update of heartbeats, fetching new jobs, polling of synchronous API, etc.) can be configured
- All routes defined in Ktor are fully configurable
  - Path of the resource
  - How to receive the input
  - How to return the result
  - Validation of the input

## Getting Started

### Include it in your Project

KJobs is released in the Maven Central Repository: [![Maven Central](https://img.shields.io/maven-central/v/com.booleworks/kjobs.svg?label=Maven%20Central)](https://mvnrepository.com/artifact/com.booleworks/kjobs)

For all the examples in this guide you will also need at least a dependency to a Ktor web service engine. We're using CIO (`io.ktor:ktor-server-cio-jvm:2.3.2`). Configuring a logger like logback will also be useful. For details, you can check out the examples at https://github.com/booleworks/kjobs-examples.

### Set up a simple Web Service

```kotlin linenums="1" hl_lines="25-32"
import com.booleworks.kjobs.api.JobFramework
import com.booleworks.kjobs.api.persistence.hashmap.HashMapDataPersistence
import com.booleworks.kjobs.api.persistence.hashmap.HashMapJobPersistence
import com.booleworks.kjobs.control.ComputationResult
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

fun main() { // 1
    embeddedServer(CIO, port = 8080, host = "0.0.0.0", module = Application::module).start(wait = true)
}

fun Application.module() {
    // 2
    val jobPersistence = HashMapJobPersistence()
    val dataPersistence = HashMapDataPersistence<String, String>(jobPersistence)

    routing { // 3
        route("computation") { // 4
         /* 5 */JobFramework(myInstanceName = "ME"/* 6 */, jobPersistence/* 7 */)  {
                addApi( // 8
                    jobType = "My-first-KJobs-API", // 9
                    route = this@route, // 10
                    dataPersistence, // 11
                    inputReceiver = { call.receiveText() }, // 12
                    resultResponder = { call.respondText(it) }, // 13
                    { _, input -> ComputationResult.Success<String>("Result of $input") } // 14
                )
            }
        }
    }
}
```

1. Main method to set up the Ktor web service on localhost:8080
2. Setup persistence (this is the simplest kind of persistence which should only be used for testing purposes!)
3. initialize web service routing
4. create a basic route 'optimization'
5. initialize the KJobs `JobFramework`
6. a unique name of the instance, relevant only when using multiple instances
7. the persistence layer where jobs and their inputs and results are stored
8. add an API
9. name of the job type, only used internally to distinguish different APIs
10. the route on which the API should be generated
11. the persistence where jobs are store (together with their inputs and results)
12. how the service should accept incoming requests to the 'submit' method (will usually be some object via JSON)
13. how the service should return the result (will usually be some object via JSON)
14. how the actual computation is performed

Running the `main` method of this snippet will start a new web service on `localhost:8080` which provides the following resources:

- `POST computation/submit` which expects a random string as body and returns a UUID. Internally, it will create a new job and as soon as some instance (here we only have one) is free, the job will be computed.
- `GET computation/status/{uuid}` returns the status of the job with the given UUID. The status can be one of the following (there are actually some more states which we're skipping for now):
  - `CREATED`: The job has been created successfully, but the computation has not been started yet.
  - `RUNNING`: The computation has started, but it is not yet finished.
  - `SUCCESS`: The computation has finished successfully and the result can be collected.
- `GET computation/result/{uuid}` returns the result of the computation for the given UUID if the status is `SUCCESS`. In our simple service this result is just the string `"Result of $input"`.

Note that you are free to accept and return any kind of input (e.g. a JSON object) by altering the `inputReceiver` and `resultResponder`.
