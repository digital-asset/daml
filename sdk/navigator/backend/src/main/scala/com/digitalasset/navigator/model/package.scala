// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import scalaz.{@@, Tag}
import com.daml.lf.{data => DamlLfData}
import com.daml.lf.data.{Ref => DamlLfRef}
import com.daml.lf.{iface => DamlLfIface}
import com.daml.lf.value.json.NavigatorModelAliases
import com.daml.ledger.api.{v1 => ApiV1}
import com.daml.ledger.api.refinements.ApiTypes

package object model extends NavigatorModelAliases[String] {

  /** An opaque identifier used for templates.
    * Templates are usually identified using a composite type (see [[DamlLfIdentifier]]).
    */
  type TemplateStringId = String @@ TemplateStringIdTag
  val TemplateStringId = Tag.of[TemplateStringIdTag]

  // ----------------------------------------------------------------------------------------------
  // Types used in the ledger API
  // ----------------------------------------------------------------------------------------------

  type EventId = ApiTypes.EventId
  type ContractId = ApiTypes.ContractId
  type TemplateId = ApiTypes.TemplateId
  type Party = DamlLfRef.Party
  type CommandId = ApiTypes.CommandId
  type WorkflowId = ApiTypes.WorkflowId

  // ----------------------------------------------------------------------------------------------
  // Types used in Daml-LF
  // ----------------------------------------------------------------------------------------------

  /** A dot-separated list of strings */
  type DamlLfDottedName = DamlLfRef.DottedName
  val DamlLfDottedName = DamlLfRef.DottedName

  /** A qualified name, referencing entities from the same Daml-LF package */
  type DamlLfQualifiedName = DamlLfRef.QualifiedName
  val DamlLfQualifiedName = DamlLfRef.QualifiedName

  type DamlLfTypeConNameOrPrimType = DamlLfIface.TypeConNameOrPrimType

  type DamlLfImmArraySeq[T] = DamlLfData.ImmArray.ImmArraySeq[T]
  val DamlLfImmArraySeq = DamlLfData.ImmArray.ImmArraySeq

  type DamlLfImmArray[T] = DamlLfData.ImmArray[T]
  val DamlLfImmArray = DamlLfData.ImmArray

  type DamlLfFieldWithType = DamlLfIface.FieldWithType

  // ----------------------------------------------------------------------------------------------
  // Conversion between API Identifier, Daml-LF Identifier, and String
  // ----------------------------------------------------------------------------------------------
  implicit class IdentifierApiConversions(val id: ApiV1.value.Identifier) extends AnyVal {
    def asDaml: DamlLfRef.Identifier =
      DamlLfRef.Identifier(
        DamlLfRef.PackageId.assertFromString(id.packageId),
        DamlLfRef.QualifiedName(
          DamlLfRef.DottedName.assertFromString(id.moduleName),
          DamlLfRef.DottedName.assertFromString(id.entityName),
        ),
      )

    /** An opaque unique string for this identifier */
    def asOpaqueString: String = id.asDaml.asOpaqueString
  }

  implicit class IdentifierDamlConversions(val id: DamlLfRef.Identifier) extends AnyVal {
    def asApi: ApiV1.value.Identifier =
      ApiV1.value.Identifier(
        id.packageId,
        id.qualifiedName.module.toString(),
        id.qualifiedName.name.toString(),
      )

    /** An opaque unique string for this identifier */
    def asOpaqueString: String =
      opaqueIdentifier(id.qualifiedName.toString, id.packageId)
  }

  private[this] def opaqueIdentifier(qualifiedName: String, packageId: String): String =
    s"$qualifiedName@$packageId"

  private[this] val opaqueIdentifierRegex = "([^@]*)@([^@]*)".r
  def parseOpaqueIdentifier(id: String): Option[DamlLfRef.Identifier] = {
    id match {
      case opaqueIdentifierRegex(qualifiedName, packageId) =>
        Some(
          DamlLfRef.Identifier(
            DamlLfRef.PackageId.assertFromString(packageId),
            DamlLfRef.QualifiedName.assertFromString(qualifiedName),
          )
        )
      case _ =>
        None
    }
  }

  def parseOpaqueIdentifier(id: TemplateStringId): Option[DamlLfRef.Identifier] =
    parseOpaqueIdentifier(TemplateStringId.unwrap(id))

}

package model {
  sealed trait TemplateStringIdTag
}
