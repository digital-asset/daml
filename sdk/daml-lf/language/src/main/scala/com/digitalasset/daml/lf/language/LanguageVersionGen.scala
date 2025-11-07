// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion.Major._
import com.digitalasset.daml.lf.language.LanguageVersion.Minor._

import scala.annotation.nowarn

trait LanguageVersionGenerated {
  val allStableLegacy: List[LanguageVersion] =
    List(6, 7, 8, 11, 12, 13, 14, 15, 17).map(i => LanguageVersion(V1, Stable(i)))
  val List(v1_6, v1_7, v1_8, v1_11, v1_12, v1_13, v1_14, v1_15, v1_17) = allStableLegacy: @nowarn(
    "msg=match may not be exhaustive"
  )
  val v1_dev: LanguageVersion = LanguageVersion(V1, Dev)
  val allLegacy: List[LanguageVersion] = allStableLegacy.appended(v1_dev)

  // Start of code that in the furutre will be generated from
  // //daml-lf/language/daml-lf.bzl
  val v2_1: LanguageVersion = LanguageVersion(V2, Stable(1))
  val v2_2: LanguageVersion = LanguageVersion(V2, Stable(2))
  val v2_dev: LanguageVersion = LanguageVersion(V2, Dev)

  val latestStable: LanguageVersion = v2_2
  val default: LanguageVersion = v2_2
  val dev: LanguageVersion = v2_dev

  val all: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  val allRange = VersionRange(v2_1, v2_dev)
  val stable: List[LanguageVersion] = List(v2_1, v2_2)
  val stableRange = VersionRange(v2_1, v2_2)
  val earlyAccess = stable
  val earlyAccessRange = VersionRange(v2_1, v2_2)
  val compilerInput: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)
  val compilerOutput: List[LanguageVersion] = List(v2_1, v2_2, v2_dev)

  object Features {
    val default = v2_1
    val packageUpgrades = v2_1

    val flatArchive = v2_2
    val kindInterning = flatArchive
    val exprInterning = flatArchive

    val explicitPkgImports = v2_2

    val choiceFuncs = v2_dev
    val choiceAuthority = v2_dev

    /** TYPE_REP_TYCON_NAME builtin */
    val templateTypeRepToText = v2_dev

    /** Guards in interfaces */
    val extendedInterfaces = v2_dev

    /** BigNumeric */
    val bigNumeric = v2_dev

    val contractKeys = v2_dev

    val complexAnyType = v2_dev

    val cryptoUtility = v2_dev

    /** UNSAFE_FROM_INTERFACE is removed starting from 2.2, included */
    val unsafeFromInterfaceRemoved = v2_2

    /** Unstable, experimental features. This should stay in x.dev forever.
      * Features implemented with this flag should be moved to a separate
      * feature flag once the decision to add them permanently has been made.
      */
    val unstable = v2_dev
  }

  object LegacyFeatures {
    val default = v1_6
    val internedPackageId = v1_6
    val internedStrings = v1_7
    val internedDottedNames = v1_7
    val numeric = v1_7
    val anyType = v1_7
    val typeRep = v1_7
    val typeSynonyms = v1_8
    val packageMetadata = v1_8
    val genComparison = v1_11
    val genMap = v1_11
    val scenarioMustFailAtMsg = v1_11
    val contractIdTextConversions = v1_11
    val exerciseByKey = v1_11
    val internedTypes = v1_11
    val choiceObservers = v1_11
    val bigNumeric = v1_13
    val exceptions = v1_14
    val basicInterfaces = v1_15
    val choiceFuncs = v1_dev
    val choiceAuthority = v1_dev
    val natTypeErasure = v1_dev
    val packageUpgrades = v1_17
    val sharedKeys = v1_17
    val templateTypeRepToText = v1_dev
    val extendedInterfaces = v1_dev
    val unstable = v1_dev
  }
}
