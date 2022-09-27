// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts
package util

import com.daml.lf
import com.daml.ledger.api.{v1 => lav1}

private[daml] object IdentifierConverters {
  def apiIdentifier(a: lf.data.Ref.Identifier): lav1.value.Identifier =
    lav1.value.Identifier(
      packageId = a.packageId,
      moduleName = a.qualifiedName.module.dottedName,
      entityName = a.qualifiedName.name.dottedName,
    )

  def apiIdentifier(a: domain.ContractTypeId.RequiredPkg): lav1.value.Identifier =
    lav1.value.Identifier(
      packageId = a.packageId,
      moduleName = a.moduleName,
      entityName = a.entityName,
    )
}
