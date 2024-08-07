// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs.data

import com.booleworks.kjobs.data.ExecutionCapacity.Companion.AcceptingAnyJob
import com.booleworks.kjobs.data.ExecutionCapacity.Companion.AcceptingNoJob

/**
 * Interface for specifying the execution capacity of an instance.
 *
 * The idea is that an instance may only be free for some (small or high prioritized)
 * jobs, depending on the current workload of the instance.
 *
 * An [ExecutionCapacity] is typically generated by a [ExecutionCapacityProvider], i.e.
 * a function which receives all jobs it is currently running and from that it can
 * decide whether and which kind of jobs it is able to accept.
 */
interface ExecutionCapacity {
    /**
     * `true` if there can be any job for which [isSufficientFor] would return `true`.
     */
    val mayTakeJobs: Boolean

    /**
     * Returns `true` if this instance is able to run the given job.
     */
    fun isSufficientFor(job: Job): Boolean

    companion object {
        /**
         * An execution capacity specifying that the instance will accept any job.
         */
        object AcceptingAnyJob : ExecutionCapacity {
            override val mayTakeJobs = true
            override fun isSufficientFor(job: Job) = true
        }

        /**
         * An execution capacity specifying that the instance will accept no job.
         */
        object AcceptingNoJob : ExecutionCapacity {
            override val mayTakeJobs = false
            override fun isSufficientFor(job: Job) = false
        }
    }
}

/**
 * Takes a (possibly empty) list of jobs which are currently running on this instance and
 * returns an execution capacity.
 */
fun interface ExecutionCapacityProvider {
    operator fun invoke(runningJobs: List<Job>): ExecutionCapacity
}

/**
 * Return the job with the highest priority from a given list of jobs.
 * If the list of jobs is not empty, the result must not be `null`.
 */
fun interface JobPrioritizer {
    operator fun invoke(jobs: List<Job>): Job?
}

/**
 * The default execution capacity provider which returns [AcceptingAnyJob] if the instance
 * is not computing anything and otherwise [AcceptingNoJob].
 */
val DefaultExecutionCapacityProvider: ExecutionCapacityProvider = ExecutionCapacityProvider { if (it.isEmpty()) AcceptingAnyJob else AcceptingNoJob }

/**
 * The default job prioritizer taking the job with the highest priority (i.e. lowest number)
 * and, if there are multiple of such jobs, the one with the earliest creation date.
 */
val DefaultJobPrioritizer: JobPrioritizer = JobPrioritizer { jobs -> jobs.minWithOrNull(compareBy({ it.priority }, { it.createdAt })) }

/**
 * A tag matcher specifies whether an instance can compute jobs with the given list of tags.
 *
 * There are already predefined implementations:
 * - [TagMatcher.Any]
 * - [TagMatcher.OneOf]
 * - [TagMatcher.AllOf]
 * - [TagMatcher.Exactly]
 *
 * However, you are free to create your own tag matcher which just needs to implement the [matches]
 * method. Not that you are actually not limited to use the tags, but you can also use other attributes
 * of Job.
 */
fun interface TagMatcher {

    /**
     * Returns `true` if the job can be computed by this instance.
     */
    fun matches(job: Job): Boolean

    /**
     * A tag matcher matching all jobs.
     */
    object Any : TagMatcher {
        override fun matches(job: Job) = true
    }

    /**
     * A tag matcher matching jobs which have *at least* one of the [desiredTags].
     *
     * Note that jobs with an empty list of tags will *never* be matched this type of tag matcher.
     */
    class OneOf(private vararg val desiredTags: String) : TagMatcher {
        override fun matches(job: Job) = desiredTags.intersect(job.tags.toSet()).isNotEmpty()
    }

    /**
     * A tag matcher matching jobs which have all of the [desiredTags] (and they may have more).
     *
     * If [desiredTags] are empty, this type of tag matcher behaves exactly like [TagMatcher.Any].
     */
    class AllOf(private vararg val desiredTags: String) : TagMatcher {
        override fun matches(job: Job) = job.tags.toSet().containsAll(desiredTags.toList())
    }

    /**
     * A tag matcher matching all jobs which have exactly the [desiredTags] (the order of the tags does not matter).
     */
    class Exactly(private vararg val desiredTags: String) : TagMatcher {
        override fun matches(job: Job) = job.tags.toSet() == desiredTags.toSet()
    }
}
