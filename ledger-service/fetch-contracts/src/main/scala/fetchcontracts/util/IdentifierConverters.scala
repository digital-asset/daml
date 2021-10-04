// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts
package util

import com.daml.lf
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => lav1}

private[daml] object IdentifierConverters {
  def apiIdentifier(a: lf.data.Ref.Identifier): lav1.value.Identifier =
    lav1.value.Identifier(
      packageId = a.packageId,
      moduleName = a.qualifiedName.module.dottedName,
      entityName = a.qualifiedName.name.dottedName,
    )

  @deprecated("TemplateId is a tag, use unwrap or unsubst instead", since = "1.18.0")
  def apiIdentifier(a: lar.TemplateId): lav1.value.Identifier = lar.TemplateId.unwrap(a)

  def apiIdentifier(a: domain.TemplateId.RequiredPkg): lav1.value.Identifier =
    lav1.value.Identifier(
      packageId = a.packageId,
      moduleName = a.moduleName,
      entityName = a.entityName,
    )
}
