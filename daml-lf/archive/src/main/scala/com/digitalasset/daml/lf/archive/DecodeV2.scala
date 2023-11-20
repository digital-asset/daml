// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.SafeProto
import com.daml.daml_lf_dev.{DamlLf1 => Lf1}
import com.daml.daml_lf_dev.{DamlLf2 => Lf2}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion => LV}

/** Decodes LF2 packages and modules.
  *
  * The DamlLf2 proto is currently a copy of DamlLf1 so we can coerce one to the other and delegate
  * the decoding to [[DecodeCommon]], which consumes DamlLF1 protos. As the protos start diverging,
  * we will migrate some parts of the conversion into [[DecodeV1]] and [[DecodeV2]] and keep the
  * common bits in DecodeCommon.
  */
private[archive] class DecodeV2(minor: LV.Minor) {

  private val decodeCommon = new DecodeCommon(new LV(LV.Major.V2, minor))

  def decodePackage(
      packageId: PackageId,
      lf2PackagePb: Lf2.Package,
      onlySerializableDataDefs: Boolean,
  ): Either[Error, Package] =
    for {
      lf1PackagePb <- coerce(lf2PackagePb)
      result <- decodeCommon
        .decodePackage(packageId, lf1PackagePb, onlySerializableDataDefs)
    } yield result

  // each LF scenario module is wrapped in a distinct proto package
  type ProtoScenarioModule = Lf2.Package

  def decodeScenarioModule( // entry point
      packageId: PackageId,
      lf2ScenarioModule: ProtoScenarioModule,
  ): Either[Error, Module] =
    for {
      lf1ScenarioModule <- coerce(lf2ScenarioModule)
      result <- decodeCommon.decodeScenarioModule(packageId, lf1ScenarioModule)
    } yield result

  /** Converts an LF2 proto into an LF1 proto, relying on the fact that the two proto definitions
    * are identical for now.
    */
  def coerce(lf2PackagePb: Lf2.Package): Either[Error, Lf1.Package] =
    SafeProto
      .toByteString(lf2PackagePb)
      .left
      .map(msg => Error.Internal("SafeProto.toByteString", msg, None))
      .flatMap(Lf1PackageParser.fromByteString)
}
