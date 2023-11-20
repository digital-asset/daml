// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.daml_lf_dev.{DamlLf1 => PLF}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion => LV}

private[archive] class DecodeV1(minor: LV.Minor) {

  private val decodeCommon = new DecodeCommon(LV(LV.Major.V1, minor))

  def decodePackage( // entry point
      packageId: PackageId,
      lfPackage: PLF.Package,
      onlySerializableDataDefs: Boolean,
  ): Either[Error, Package] =
    decodeCommon.decodePackage(packageId, lfPackage, onlySerializableDataDefs)

  // each LF scenario module is wrapped in a distinct proto package
  type ProtoScenarioModule = PLF.Package

  def decodeScenarioModule( // entry point
      packageId: PackageId,
      lfScenarioModule: ProtoScenarioModule,
  ): Either[Error, Module] =
    decodeCommon.decodeScenarioModule(packageId, lfScenarioModule)
}
