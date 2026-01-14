// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//
// THIS FILE WAS GENERATED FROM //canton/community/daml-lf/language/daml-lf.bzl
// (via generate_features.py)
//
// IT IS CHECKED IN AS A TEMPORARY MEASURE FOR THE MIGRATION OF LF/ENGINE TO THE
// CANTON REPO IN THE FUTURE, IT WILL BE AUTO-GENERATED ONCE MORE
//

package com.digitalasset.daml.lf
package language

import com.digitalasset.daml.lf.language.LanguageVersion.Feature

trait LanguageFeaturesGenerated extends LanguageVersionGenerated {

  // TODO (FEATURE): Remove this hardcoded object once V1 features are also generated
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

  // --- Generated V2 Features ---

  val featureUnstable: Feature = Feature(
    name = "Unstable, experimental features",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureTextMap: Feature = Feature(
    name = "TextMap type",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureBigNumeric: Feature = Feature(
    name = "BigNumeric type",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExceptions: Feature = Feature(
    name = "Daml Exceptions",
    versionRange = VersionRange.From(v2_1),
  )

  val featureExtendedInterfaces: Feature = Feature(
    name = "Guards in interfaces",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureChoiceFuncs: Feature = Feature(
    name = "choiceController and choiceObserver functions",
    versionRange = VersionRange.Empty(),
  )

  val featureTemplateTypeRepToText: Feature = Feature(
    name = "templateTypeRepToText function",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureContractKeys: Feature = Feature(
    name = "Contract Keys",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureFlatArchive: Feature = Feature(
    name = "Flat Archive",
    versionRange = VersionRange.From(v2_2),
  )

  val featurePackageImports: Feature = Feature(
    name = "Explicit package imports",
    versionRange = VersionRange.From(v2_2),
  )

  val featureComplexAnyType: Feature = Feature(
    name = "Complex Any type",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureExperimental: Feature = Feature(
    name = "Daml Experimental",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featurePackageUpgrades: Feature = Feature(
    name = "Package upgrades",
    versionRange = VersionRange.From(v2_1),
  )

  val featureChoiceAuthority: Feature = Feature(
    name = "Choice Authorizers",
    versionRange = VersionRange.Inclusive(v2_dev, v2_dev),
  )

  val featureUnsafeFromInterface: Feature = Feature(
    name = "UnsafeFromInterface builtin",
    versionRange = VersionRange.Until(v2_1),
  )

  val allFeatures: List[Feature] = List(
    featureUnstable,
    featureTextMap,
    featureBigNumeric,
    featureExceptions,
    featureExtendedInterfaces,
    featureChoiceFuncs,
    featureTemplateTypeRepToText,
    featureContractKeys,
    featureFlatArchive,
    featurePackageImports,
    featureComplexAnyType,
    featureExperimental,
    featurePackageUpgrades,
    featureChoiceAuthority,
    featureUnsafeFromInterface,
  )

}
