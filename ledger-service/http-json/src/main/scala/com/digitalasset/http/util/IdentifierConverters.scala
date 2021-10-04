// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.fetchcontracts.util.{IdentifierConverters => FC}
import com.daml.lf
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => lav1}
import com.daml.http

object IdentifierConverters {

  def lfIdentifier(a: lar.TemplateId): lf.data.Ref.Identifier =
    lfIdentifier(lar.TemplateId.unwrap(a))

  def lfIdentifier(a: lav1.value.Identifier): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(a.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      ),
    )
  }

  def lfIdentifier(a: http.domain.TemplateId.RequiredPkg): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(a.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      ),
    )
  }

  def apiIdentifier(a: lf.data.Ref.Identifier): lav1.value.Identifier =
    FC.apiIdentifier(a)

  @deprecated("TemplateId is a tag, use unwrap or unsubst instead", since = "1.18.0")
  def apiIdentifier(a: lar.TemplateId): lav1.value.Identifier = FC.apiIdentifier(a)

  def apiIdentifier(a: http.domain.TemplateId.RequiredPkg): lav1.value.Identifier =
    FC.apiIdentifier(a)

  def refApiIdentifier(a: http.domain.TemplateId.RequiredPkg): lar.TemplateId =
    lar.TemplateId(apiIdentifier(a))

  def apiLedgerId(a: com.daml.ledger.api.domain.LedgerId): lar.LedgerId =
    lar.LedgerId(com.daml.ledger.api.domain.LedgerId.unwrap(a))
}
