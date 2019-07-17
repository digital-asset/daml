// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.daml.lf
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}

object DamlLfIdentifiers {

  def damlLfIdentifier(a: lar.TemplateId): lf.data.Ref.Identifier =
    damlLfIdentifier(lar.TemplateId.unwrap(a))

  def damlLfIdentifier(a: lav1.value.Identifier): lf.data.Ref.Identifier = {
    import lf.data.Ref
    Ref.Identifier(
      Ref.PackageId.assertFromString(a.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(a.moduleName),
        Ref.DottedName.assertFromString(a.entityName))
    )
  }
}
