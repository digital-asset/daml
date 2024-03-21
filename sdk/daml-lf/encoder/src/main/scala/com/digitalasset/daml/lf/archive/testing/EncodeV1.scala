// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive
package testing

import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LanguageVersion => LV}
import com.daml.daml_lf_dev.{DamlLf1 => PLF}

// Important: do not use this in production code. It is designed for testing only.
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
private[daml] class EncodeV1(minor: LV.Minor) {

  private val encodeCommon = new EncodeCommon(LV(LV.Major.V1, minor))

  def encodePackage(pkgId: PackageId, pkg: Package): PLF.Package = {
    encodeCommon.encodePackage(pkgId, pkg)
  }
}
