// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.binding.Primitive as P

final case class TemplateId(
    packageId: String,
    moduleName: String,
    entityName: String,
) {
  def toIdentifier: Identifier = Identifier(
    packageId = packageId,
    moduleName = moduleName,
    entityName = entityName,
  )

  def toPrim: P.TemplateId[_] = P.TemplateId(
    packageId = packageId,
    moduleName = moduleName,
    entityName = entityName,
  )

  def isModuleEntity(moduleName: String, entityName: String) =
    this.moduleName == moduleName && this.entityName == entityName
}

object TemplateId {

  def fromIdentifier(identifier: Identifier): TemplateId = {
    TemplateId(
      packageId = identifier.packageId,
      moduleName = identifier.moduleName,
      entityName = identifier.entityName,
    )
  }

  def templateIds(apiTemplateIds: ApiTypes.TemplateId*): Seq[TemplateId] = {
    apiTemplateIds.map(fromPrim)
  }

  def fromPrim(templateId: ApiTypes.TemplateId): TemplateId = {
    import scalaz.syntax.tag.*
    fromIdentifier(templateId.unwrap)
  }

}
