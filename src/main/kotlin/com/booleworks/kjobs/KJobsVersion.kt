// SPDX-License-Identifier: MIT
// Copyright 2023-2024 BooleWorks GmbH

package com.booleworks.kjobs

/**
 * Contains version information about this framework (according to the pom file).
 */
object KJobsVersion {
    /**
     * The full version or `null` if the version could not be parsed.
     */
    val version: String? = runCatching { KJobsVersion::class.java.getResource("version.txt")?.readText()?.trim() }.getOrNull()

    /**
     * The major version or `null` if the version could not be parsed.
     */
    val major: Int? = version?.split(".")?.getOrNull(0)?.toIntOrNull()

    /**
     * The minor version or `null` if the version could not be parsed.
     */
    val minor: Int? = version?.split(".")?.getOrNull(1)?.toIntOrNull()

    /**
     * The patch version or `null` if the version could not be parsed.
     */
    val patch: Int? = version?.split(".")?.getOrNull(2)?.split("-")?.getOrNull(0)?.toIntOrNull()

    /**
     * The suffix (pre-release, hotfix, etc) or empty if not present or `null` if the version could not be parsed.
     */
    val suffix: String? = version?.split("-")?.getOrElse(1) { "" }
}
