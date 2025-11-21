// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//
// THIS FILE IS-GENERATED FROM //daml-lf/language/daml-lf.bzl
// (via generate_features.py)
// DO NOT EDIT MANUALLY
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
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_UNSTABLE",
  )

  val featureTextMap: Feature = Feature(
    name = "TextMap type",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_TEXTMAP",
  )

  val featureBigNumeric: Feature = Feature(
    name = "BigNumeric type",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_BIGNUMERIC",
  )

  val featureExceptions: Feature = Feature(
    name = "Daml Exceptions",
    versionReq = VersionRange.From(v2_1),
    cppFlag = "DAML_EXCEPTIONS",
  )

  val featureExtendedInterfaces: Feature = Feature(
    name = "Guards in interfaces",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_INTERFACE_EXTENDED",
  )

  val featureChoiceFuncs: Feature = Feature(
    name = "choiceController and choiceObserver functions",
    versionReq = VersionRange.Empty(),
    cppFlag = "DAML_CHOICE_FUNCS",
  )

  val featureTemplateTypeRepToText: Feature = Feature(
    name = "templateTypeRepToText function",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_TEMPLATE_TYPEREP_TO_TEXT",
  )

  val featureContractKeys: Feature = Feature(
    name = "Contract Keys",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_CONTRACT_KEYS",
  )

  val featureFlatArchive: Feature = Feature(
    name = "Flat Archive",
    versionReq = VersionRange.From(v2_2),
    cppFlag = "DAML_FLATARCHIVE",
  )

  val featurePackageImports: Feature = Feature(
    name = "Explicit package imports",
    versionReq = VersionRange.From(v2_2),
    cppFlag = "DAML_PackageImports",
  )

  val featureComplexAnyType: Feature = Feature(
    name = "Complex Any type",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_COMPLEX_ANY_TYPE",
  )

  val featureExperimental: Feature = Feature(
    name = "Daml Experimental",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_EXPERIMENTAL",
  )

  val featurePackageUpgrades: Feature = Feature(
    name = "Package upgrades",
    versionReq = VersionRange.From(v2_1),
    cppFlag = "DAML_PackageUpgrades",
  )

  val featureChoiceAuthority: Feature = Feature(
    name = "ChoiceAuthority???",
    versionReq = VersionRange.Inclusive(v2_dev, v2_dev),
    cppFlag = "DAML_ChoiceAuthority",
  )

  val featureUnsafeFromInterface: Feature = Feature(
    name = "UnsafeFromInterface???",
    versionReq = VersionRange.Until(v2_1),
    cppFlag = "DAML_UnsafeFromInterface",
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
