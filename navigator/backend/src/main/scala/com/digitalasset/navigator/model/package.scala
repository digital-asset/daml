// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import scalaz.{@@, Tag}
import com.digitalasset.daml.lf.{data => DamlLfData}
import com.digitalasset.daml.lf.data.{Ref => DamlLfRef}
import com.digitalasset.daml.lf.{iface => DamlLfIface}
import com.digitalasset.daml.lf.value.json.NavigatorModelAliases
import com.digitalasset.ledger.api.{v1 => ApiV1}
import com.digitalasset.ledger.api.refinements.ApiTypes

package object model extends NavigatorModelAliases[String] {

  /**
    * An opaque identifier used for templates.
    * Templates are usually identified using a composite type (see [[DamlLfIdentifier]]).
    */
  sealed trait TemplateStringIdTag
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
  // Types used in DAML-LF
  // ----------------------------------------------------------------------------------------------

  /** A dot-separated list of strings */
  type DamlLfDottedName = DamlLfRef.DottedName
  val DamlLfDottedName = DamlLfRef.DottedName

  /** A qualified name, referencing entities from the same DAML-LF package */
  type DamlLfQualifiedName = DamlLfRef.QualifiedName
  val DamlLfQualifiedName = DamlLfRef.QualifiedName

  type DamlLfTypeConNameOrPrimType = DamlLfIface.TypeConNameOrPrimType

  type DamlLfImmArraySeq[T] = DamlLfData.ImmArray.ImmArraySeq[T]
  val DamlLfImmArraySeq = DamlLfData.ImmArray.ImmArraySeq

  type DamlLfImmArray[T] = DamlLfData.ImmArray[T]
  val DamlLfImmArray = DamlLfData.ImmArray

  type DamlLfFieldWithType = DamlLfIface.FieldWithType

  // ----------------------------------------------------------------------------------------------
  // Conversion between API Identifier, DAML-LF Identifier, and String
  // ----------------------------------------------------------------------------------------------
  implicit class IdentifierApiConversions(val id: ApiV1.value.Identifier) extends AnyVal {
    def asDaml: DamlLfRef.Identifier =
      DamlLfRef.Identifier(
        DamlLfRef.PackageId.assertFromString(id.packageId),
        DamlLfRef.QualifiedName(
          DamlLfRef.DottedName.assertFromString(id.moduleName),
          DamlLfRef.DottedName.assertFromString(id.entityName))
      )

    /** An opaque unique string for this identifier */
    def asOpaqueString: String = id.asDaml.asOpaqueString
  }

  implicit class IdentifierDamlConversions(val id: DamlLfRef.Identifier) extends AnyVal {
    def asApi: ApiV1.value.Identifier =
      ApiV1.value.Identifier(
        id.packageId,
        id.qualifiedName.module.toString(),
        id.qualifiedName.name.toString())

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
            DamlLfRef.QualifiedName.assertFromString(qualifiedName)))
      case _ =>
        None
    }
  }

  def parseOpaqueIdentifier(id: TemplateStringId): Option[DamlLfRef.Identifier] =
    parseOpaqueIdentifier(TemplateStringId.unwrap(id))
}
