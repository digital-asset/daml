// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.daml.ledger.api.v1.ValueOuterClass
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.javaapi

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

  def toJavaIdentifier: javaapi.data.Identifier = new javaapi.data.Identifier(
    packageId,
    moduleName,
    entityName,
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

  def templateIdsFromJava(identifiers: javaapi.data.Identifier*): Seq[TemplateId] = {
    identifiers.map(fromJavaIdentifier)
  }

  def fromJavaProtoIdentifier(templateId: ValueOuterClass.Identifier): TemplateId = {
    fromIdentifier(Identifier.fromJavaProto(templateId))
  }

  def fromJavaIdentifier(templateId: javaapi.data.Identifier): TemplateId = {
    fromJavaProtoIdentifier(templateId.toProto)
  }

}
