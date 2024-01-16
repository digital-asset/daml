// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive
package testing

import com.daml.SafeProto
import com.daml.daml_lf_dev.{DamlLf2 => PLF}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion => LV}

// Important: do not use this in production code. It is designed for testing only.
private[daml] class EncodeV2(minor: LV.Minor) {
  def encodePackage(pkgId: PackageId, pkg: Package): PLF.Package = {
    // The DamlLf2 proto is currently a copy of DamlLf1 so we can coerce one to the other.
    // TODO (#17366): Stop delegating to EncodeV1 once the two proto definitions diverge or when
    //  certain features become exclusive to 1.x or 2.x.
    val lf1PackagePb = new EncodeCommon(LV(LV.Major.V2, minor)).encodePackage(pkgId, pkg)
    SafeProto
      .toByteString(lf1PackagePb)
      .left
      .map(msg => Error.Internal("SafeProto.toByteString", msg, None))
      .flatMap(Lf2PackageParser.fromByteString)
      .fold(
        err => throw new RuntimeException(err),
        identity,
      )
  }
}
