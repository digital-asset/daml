// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.daml.lf
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.digitalasset.http

object IdentifierConverters {

  def lfIdentifier(a: lar.TemplateId): lf.data.Ref.Identifier =
    lfIdentifier(lar.TemplateId.unwrap(a))

  def lfIdentifier(a: lav1.value.Identifier): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(a.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName))
    )
  }

  def lfIdentifier(a: http.domain.TemplateId.RequiredPkg): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(a.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName))
    )
  }

  def lfIdentifier(
      id: http.domain.TemplateId.RequiredPkg,
      choice: lar.Choice): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(id.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(id.moduleName),
        Ref.DottedName.assertFromString(lar.Choice.unwrap(choice)))
    )
  }

  def apiIdentifier(a: lf.data.Ref.Identifier): lav1.value.Identifier =
    lav1.value.Identifier(
      packageId = a.packageId,
      moduleName = a.qualifiedName.module.dottedName,
      entityName = a.qualifiedName.name.dottedName)

  def apiIdentifier(a: lar.TemplateId): lav1.value.Identifier = lar.TemplateId.unwrap(a)

  def apiIdentifier(a: http.domain.TemplateId.RequiredPkg): lav1.value.Identifier =
    lav1.value.Identifier(
      packageId = a.packageId,
      moduleName = a.moduleName,
      entityName = a.entityName)

  def refApiIdentifier(a: http.domain.TemplateId.RequiredPkg): lar.TemplateId =
    lar.TemplateId(apiIdentifier(a))

  def apiLedgerId(a: com.digitalasset.ledger.api.domain.LedgerId): lar.LedgerId =
    lar.LedgerId(com.digitalasset.ledger.api.domain.LedgerId.unwrap(a))
}
