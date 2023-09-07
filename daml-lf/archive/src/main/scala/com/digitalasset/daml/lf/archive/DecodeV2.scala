// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.daml.SafeProto
import com.daml.daml_lf_dev.{DamlLf2 => PLF}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion.V2
import com.daml.lf.language.{LanguageVersion => LV}

private[archive] class DecodeV2(minor: LV.Minor) {

  def decodePackage(
      packageId: PackageId,
      lf2PackagePb: PLF.Package,
      onlySerializableDataDefs: Boolean,
  ): Either[Error, Package] = {
    for {
      // The DamlLf2 proto is currently a copy of DamlLf1 so we can coerce one to the other.
      // TODO (#17366): Stop delegating to DecodeV1 once the two proto definitions diverge or when
      //  certain features become exclusive to 1.x or 2.x.
      lf2ByteString <- SafeProto
        .toByteString(lf2PackagePb)
        .left
        .map(msg => Error.Internal("SafeProto.toByteString", msg, None))
      lf1PackagePb <- Lf1PackageParser.fromByteString(lf2ByteString)
      result <- new DecodeV1(minor)
        .decodePackage(packageId, lf1PackagePb, onlySerializableDataDefs)
        .map(_.copy(languageVersion = LV(V2, minor)))
    } yield result
  }
}
