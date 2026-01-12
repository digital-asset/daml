// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//
// THIS FILE WAS GENERATED FROM //canton/community/daml-lf/language/daml-lf.bzl
// DO NOT EDIT MANUALLY
//
// IT IS CHECKED IN AS A TEMPORARY MEASURE FOR THE MIGRATION OF LF/ENGINE TO THE
// CANTON REPO IN THE FUTURE, IT WILL BE AUTO-GENERATED ONCE MORE
//

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion.Major._
import com.digitalasset.daml.lf.language.LanguageVersion.Minor._

import scala.annotation.nowarn

trait LanguageVersionGenerated {
  val allStableLegacyLfVersions: List[LanguageVersion] =
    List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(V1, Stable(i)))
  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17) =
    allStableLegacyLfVersions: @nowarn(
      "msg=match may not be exhaustive"
    )
  val v1_dev: LanguageVersion = LanguageVersion(V1, Dev)
  val allLegacyLfVersions: List[LanguageVersion] = allStableLegacyLfVersions.appended(v1_dev)

  // Start of code generated from //canton/community/daml-lf/language/daml-lf.bzl

  val v2_1: LanguageVersion = LanguageVersion(V2, Stable(1))
  val v2_2: LanguageVersion = LanguageVersion(V2, Stable(2))
  val v2_dev: LanguageVersion = LanguageVersion(V2, Dev)

  val allLfVersions: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  val compilerInputLfVersions: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  val compilerOutputLfVersions: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  val defaultLfVersion: LanguageVersion = v2_2
  val devLfVersion: LanguageVersion = v2_dev
  val latestStableLfVersion: LanguageVersion = v2_2
  val stableLfVersions: List[LanguageVersion] = List(v2_1, v2_2)
  val stagingLfVersion: LanguageVersion = v2_2

  // ranges hardcoded (for now)
  val allLfVersionsRange: VersionRange.Inclusive[LanguageVersion] = VersionRange(v2_1, v2_dev)
  val stableLfVersionsRange: VersionRange.Inclusive[LanguageVersion] = VersionRange(v2_1, v2_2)
  val earlyAccessLfVersionsRange: VersionRange.Inclusive[LanguageVersion] = VersionRange(v2_1, v2_2)
}
