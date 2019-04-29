// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import scalaz.{@@, Tag}
import com.digitalasset.daml.lf.{data => DamlLfData}
import com.digitalasset.daml.lf.data.{Ref => DamlLfRef}
import com.digitalasset.daml.lf.{iface => DamlLfIface}
import com.digitalasset.ledger.api.{v1 => ApiV1}
import com.digitalasset.ledger.api.refinements.ApiTypes

package object model {

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
  type Party = ApiTypes.Party
  type CommandId = ApiTypes.CommandId
  type WorkflowId = ApiTypes.WorkflowId

  // ----------------------------------------------------------------------------------------------
  // Types used in DAML-LF
  // ----------------------------------------------------------------------------------------------

  type DamlLfPackageId = DamlLfRef.PackageId
  val DamlLfPackageId = DamlLfRef.PackageId

  type DamlLfSimpleString = DamlLfRef.SimpleString
  val DamlLfSimpleString = DamlLfRef.SimpleString

  type DamlLfParty = DamlLfRef.Party
  val DamlLfParty = DamlLfRef.Party

  /** A dot-separated list of strings */
  type DamlLfDottedName = DamlLfRef.DottedName
  val DamlLfDottedName = DamlLfRef.DottedName

  /** A qualified name, referencing entities from the same DAML-LF package */
  type DamlLfQualifiedName = DamlLfRef.QualifiedName
  val DamlLfQualifiedName = DamlLfRef.QualifiedName

  /**
    * An absolute identifier of a DAML-LF entity.
    * Contains a DAML-LF package ID and a qualified name.
    * Currently, such identifiers can point to:
    * - Templates
    * - User-defined records
    * - User-defined variants
    */
  type DamlLfIdentifier = DamlLfRef.Identifier
  val DamlLfIdentifier = DamlLfRef.Identifier

  /**
    * A simple DAML-LF type
    * Currently, these can be:
    * - Primitive types
    * - Type constructor applications (i.e., dereferencing a DamlLfIdentifier)
    * - Type variables
    */
  type DamlLfType = DamlLfIface.Type
  type DamlLfTypeCon = DamlLfIface.TypeCon
  val DamlLfTypeCon = DamlLfIface.TypeCon
  type DamlLfTypePrim = DamlLfIface.TypePrim
  val DamlLfTypePrim = DamlLfIface.TypePrim
  type DamlLfTypeVar = DamlLfIface.TypeVar
  val DamlLfTypeVar = DamlLfIface.TypeVar
  type DamlLfTypeConName = DamlLfIface.TypeConName
  val DamlLfTypeConName = DamlLfIface.TypeConName

  type DamlLfTypeConNameOrPrimType = DamlLfIface.TypeConNameOrPrimType

  type DamlLfPrimType = DamlLfIface.PrimType
  val DamlLfPrimType = DamlLfIface.PrimType

  type DamlLfImmArraySeq[T] = DamlLfData.ImmArray.ImmArraySeq[T]
  val DamlLfImmArraySeq = DamlLfData.ImmArray.ImmArraySeq

  type DamlLfImmArray[T] = DamlLfData.ImmArray[T]
  val DamlLfImmArray = DamlLfData.ImmArray

  /** A user-defined DAML-LF type (generic form). Can be a record or variant. */
  type DamlLfDefDataType = DamlLfIface.DefDataType.FWT
  val DamlLfDefDataType = DamlLfIface.DefDataType

  /** A user-defined DAML-LF type (closed form). Can be a record or variant. */
  type DamlLfDataType = DamlLfIface.DataType.FWT
  val DamlLfDataType = DamlLfIface.DataType

  /** A user-defined DAML-LF record */
  type DamlLfRecord = DamlLfIface.Record.FWT
  val DamlLfRecord = DamlLfIface.Record

  /** A user-defined DAML-LF variant */
  type DamlLfVariant = DamlLfIface.Variant.FWT
  val DamlLfVariant = DamlLfIface.Variant

  type DamlLfFieldWithType = DamlLfIface.FieldWithType

  type DamlLfTypeLookup = DamlLfIdentifier => Option[DamlLfDefDataType]

  def damlLfInstantiate(typeCon: DamlLfTypeCon, defn: DamlLfDefDataType): DamlLfDataType =
    if (defn.typeVars.length != typeCon.typArgs.length) {
      throw new RuntimeException(
        s"Mismatching type vars and applied types, expected ${defn.typeVars} but got ${typeCon.typArgs} types")
    } else {
      if (defn.typeVars.isEmpty) { // optimization
        defn.dataType
      } else {
        val paramsMap = Map(defn.typeVars.zip(typeCon.typArgs): _*)
        def mapTypeVars(typ: DamlLfType, f: DamlLfTypeVar => DamlLfType): DamlLfType = typ match {
          case t @ DamlLfTypeVar(_) => f(t)
          case t @ DamlLfTypeCon(_, _) => DamlLfTypeCon(t.name, t.typArgs.map(mapTypeVars(_, f)))
          case t @ DamlLfTypePrim(_, _) => DamlLfTypePrim(t.typ, t.typArgs.map(mapTypeVars(_, f)))
        }
        val withTyp: DamlLfIface.Type => DamlLfIface.Type = { typ =>
          mapTypeVars(typ, v => paramsMap.getOrElse(v.name, v))
        }
        defn.dataType.bimap(withTyp, withTyp)
      }
    }

  // ----------------------------------------------------------------------------------------------
  // Conversion between API Identifier, DAML-LF Identifier, and String
  // ----------------------------------------------------------------------------------------------
  implicit class IdentifierApiConversions(val id: ApiV1.value.Identifier) extends AnyVal {
    def asDaml: DamlLfRef.Identifier =
      DamlLfRef.Identifier(
        DamlLfPackageId.assertFromString(id.packageId),
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
        id.packageId.underlyingString,
        "",
        id.qualifiedName.module.toString(),
        id.qualifiedName.name.toString())

    /** An opaque unique string for this identifier */
    def asOpaqueString: String =
      opaqueIdentifier(id.qualifiedName.toString, id.packageId.underlyingString)
  }

  private[this] def opaqueIdentifier(qualifiedName: String, packageId: String): String =
    s"$qualifiedName@$packageId"

  private[this] val opaqueIdentifierRegex = "([^@]*)@([^@]*)".r
  def parseOpaqueIdentifier(id: String): Option[DamlLfRef.Identifier] = {
    id match {
      case opaqueIdentifierRegex(qualifiedName, packageId) =>
        Some(
          DamlLfRef.Identifier(
            DamlLfPackageId.assertFromString(packageId),
            DamlLfRef.QualifiedName.assertFromString(qualifiedName)))
      case _ =>
        None
    }
  }

  def parseOpaqueIdentifier(id: TemplateStringId): Option[DamlLfRef.Identifier] =
    parseOpaqueIdentifier(TemplateStringId.unwrap(id))
}
