// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.ledger.api.v2 as lav2
import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters as FC
import com.digitalasset.canton.http
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.daml.lf

object IdentifierConverters {

  def lfIdentifier(a: lar.TemplateId): lf.data.Ref.Identifier =
    lfIdentifier(lar.TemplateId.unwrap(a))

  def lfIdentifier(a: lav2.value.Identifier): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(a.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      ),
    )
  }

  def lfIdentifier(a: http.ContractTypeId.RequiredPkgId): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      a.packageId,
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName),
      ),
    )
  }

  def refApiIdentifier(a: http.ContractTypeId.RequiredPkg): lar.TemplateId =
    lar.TemplateId(FC.apiIdentifier(a))

}
