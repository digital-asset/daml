// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.lf
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.daml.ledger.api.v1 as lav1
import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters as FC
import com.digitalasset.canton.http

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

  def lfIdentifier(a: http.domain.ContractTypeId.RequiredPkg): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(a.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      ),
    )
  }

  def refApiIdentifier(a: http.domain.ContractTypeId.RequiredPkg): lar.TemplateId =
    lar.TemplateId(FC.apiIdentifier(a))

  def apiLedgerId(a: com.digitalasset.canton.ledger.api.domain.LedgerId): lar.LedgerId =
    lar.LedgerId(com.digitalasset.canton.ledger.api.domain.LedgerId.unwrap(a))
}
