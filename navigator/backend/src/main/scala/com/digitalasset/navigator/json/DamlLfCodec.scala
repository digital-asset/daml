// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.json

import com.daml.lf.data.{Ref => DamlLfRef, Numeric => DamlLfNumeric}
import com.daml.navigator.json.Util._
import com.daml.navigator.{model => Model}
import spray.json._

/** An encoding of Daml-LF types.
  *
  * The types are encoded as-is (no type variables substitution, no type reference resolution).
  */
object DamlLfCodec {

  // ------------------------------------------------------------------------------------------------------------------
  // Constants used in the encoding
  // ------------------------------------------------------------------------------------------------------------------
  private[this] final val propType: String = "type"
  private[this] final val propValue: String = "value"
  private[this] final val propName: String = "name"
  private[this] final val propModule: String = "module"
  private[this] final val propPackage: String = "package"
  private[this] final val propArgs: String = "args"
  private[this] final val propVars: String = "vars"
  private[this] final val propFields: String = "fields"
  private[this] final val propConstructors: String = "constructors"
  private[this] final val propScale: String = "scale"

  private[this] final val tagTypeCon: String = "typecon"
  private[this] final val tagTypeVar: String = "typevar"
  private[this] final val tagTypePrim: String = "primitive"
  private[this] final val tagTypeList: String = "list"
  private[this] final val tagTypeBool: String = "bool"
  private[this] final val tagTypeNumeric: String = "numeric"
  private[this] final val tagTypeInt64: String = "int64"
  private[this] final val tagTypeContractId: String = "contractid"
  private[this] final val tagTypeDate: String = "date"
  private[this] final val tagTypeParty: String = "party"
  private[this] final val tagTypeText: String = "text"
  private[this] final val tagTypeTimestamp: String = "timestamp"
  private[this] final val tagTypeUnit: String = "unit"
  private[this] final val tagTypeRecord: String = "record"
  private[this] final val tagTypeVariant: String = "variant"
  private[this] final val tagTypeEnum: String = "enum"
  private[this] final val tagTypeOptional: String = "optional"
  private[this] final val tagTypeTextMap: String = "textmap"
  private[this] final val tagTypeGenMap: String = "genmap"

  // ------------------------------------------------------------------------------------------------------------------
  // Encoding
  // ------------------------------------------------------------------------------------------------------------------
  def damlLfTypeToJsValue(value: Model.DamlLfType): JsValue = value match {
    case typeCon: Model.DamlLfTypeCon => damlLfTypeConToJsValue(typeCon)
    case typePrim: Model.DamlLfTypePrim => damlLfPrimToJsValue(typePrim)
    case typeVar: Model.DamlLfTypeVar => damlLfTypeVarToJsValue(typeVar)
    case typeNum: Model.DamlLfTypeNumeric => damlLfNumericToJsValue(typeNum)
  }

  def damlLfTypeConToJsValue(value: Model.DamlLfTypeCon): JsValue = {
    val id =
      Model.DamlLfIdentifier(value.name.identifier.packageId, value.name.identifier.qualifiedName)
    JsObject(
      propType -> JsString(tagTypeCon),
      propName -> damlLfIdentifierToJsValue(id),
      propArgs -> JsArray(value.typArgs.map(damlLfTypeToJsValue).toVector),
    )
  }

  def damlLfTypeVarToJsValue(value: Model.DamlLfTypeVar): JsValue = JsObject(
    propType -> JsString(tagTypeVar),
    propName -> JsString(value.name),
  )

  def damlLfPrimToJsValue(value: Model.DamlLfTypePrim): JsValue = JsObject(
    propType -> JsString(tagTypePrim),
    propName -> damlLfPrimTypeToJsValue(value.typ),
    propArgs -> JsArray(value.typArgs.map(damlLfTypeToJsValue).toVector),
  )

  def damlLfPrimTypeToJsValue(value: Model.DamlLfPrimType): JsString = value match {
    case Model.DamlLfPrimType.List => JsString(tagTypeList)
    case Model.DamlLfPrimType.ContractId => JsString(tagTypeContractId)
    case Model.DamlLfPrimType.Bool => JsString(tagTypeBool)
    case Model.DamlLfPrimType.Int64 => JsString(tagTypeInt64)
    case Model.DamlLfPrimType.Date => JsString(tagTypeDate)
    case Model.DamlLfPrimType.Party => JsString(tagTypeParty)
    case Model.DamlLfPrimType.Text => JsString(tagTypeText)
    case Model.DamlLfPrimType.Timestamp => JsString(tagTypeTimestamp)
    case Model.DamlLfPrimType.Optional => JsString(tagTypeOptional)
    case Model.DamlLfPrimType.TextMap => JsString(tagTypeTextMap)
    case Model.DamlLfPrimType.GenMap => JsString(tagTypeGenMap)
    case Model.DamlLfPrimType.Unit => JsString(tagTypeUnit)
  }

  def damlLfNumericToJsValue(typeNum: Model.DamlLfTypeNumeric): JsValue = JsObject(
    propType -> JsString(tagTypeNumeric),
    propScale -> JsNumber(typeNum.scale),
  )

  def damlLfIdentifierToJsValue(value: Model.DamlLfIdentifier): JsValue = JsObject(
    propName -> JsString(value.qualifiedName.name.toString()),
    propModule -> JsString(value.qualifiedName.module.toString()),
    propPackage -> JsString(value.packageId),
  )

  def damlLfDataTypeToJsValue(value: Model.DamlLfDataType): JsValue = value match {
    case r: Model.DamlLfRecord =>
      JsObject(
        propType -> JsString(tagTypeRecord),
        propFields -> JsArray(
          r.fields
            .map(f => JsObject(propName -> JsString(f._1), propValue -> damlLfTypeToJsValue(f._2)))
            .toVector
        ),
      )
    case v: Model.DamlLfVariant =>
      JsObject(
        propType -> JsString(tagTypeVariant),
        propFields -> JsArray(
          v.fields
            .map(f => JsObject(propName -> JsString(f._1), propValue -> damlLfTypeToJsValue(f._2)))
            .toVector
        ),
      )
    case e: Model.DamlLfEnum =>
      JsObject(
        propType -> JsString(tagTypeEnum),
        propConstructors -> JsArray(e.constructors.map(JsString(_)).toVector),
      )
  }

  def damlLfDefDataTypeToJsValue(value: Model.DamlLfDefDataType): JsValue = JsObject(
    propType -> damlLfDataTypeToJsValue(value.dataType),
    propVars -> JsArray(value.typeVars.map(JsString(_)).toVector),
  )

  // ------------------------------------------------------------------------------------------------------------------
  // Decoding
  // ------------------------------------------------------------------------------------------------------------------

  def jsValueToDamlLfType(value: JsValue): Model.DamlLfType =
    strField(value, propType, "DamlLfType") match {
      case `tagTypeCon` =>
        Model.DamlLfTypeCon(
          Model.DamlLfTypeConName(
            jsValueToDamlLfIdentifier(anyField(value, propName, "DamlLfTypeCon"))
          ),
          Model.DamlLfImmArraySeq(
            arrayField(value, propArgs, "DamlLfTypeCon").map(jsValueToDamlLfType): _*
          ),
        )
      case `tagTypeVar` => Model.DamlLfTypeVar(nameField(value, propName, "DamlLfTypeVar"))
      case `tagTypePrim` =>
        Model.DamlLfTypePrim(
          jsValueToDamlLfPrimType(strField(value, propName, "DamlLfTypePrim")),
          Model.DamlLfImmArraySeq(
            arrayField(value, propArgs, "DamlLfTypePrim").map(jsValueToDamlLfType): _*
          ),
        )
      case `tagTypeNumeric` =>
        DamlLfNumeric.Scale
          .fromLong(intField(value, propScale, "DamlLfTypeNumeric"))
          .fold[Model.DamlLfTypeNumeric](deserializationError(_), Model.DamlLfTypeNumeric)
    }

  def jsValueToDamlLfPrimType(value: String): Model.DamlLfPrimType = value match {
    case `tagTypeList` => Model.DamlLfPrimType.List
    case `tagTypeContractId` => Model.DamlLfPrimType.ContractId
    case `tagTypeBool` => Model.DamlLfPrimType.Bool
    case `tagTypeInt64` => Model.DamlLfPrimType.Int64
    case `tagTypeDate` => Model.DamlLfPrimType.Date
    case `tagTypeParty` => Model.DamlLfPrimType.Party
    case `tagTypeText` => Model.DamlLfPrimType.Text
    case `tagTypeTimestamp` => Model.DamlLfPrimType.Timestamp
    case `tagTypeUnit` => Model.DamlLfPrimType.Unit
    case `tagTypeOptional` => Model.DamlLfPrimType.Optional
    case `tagTypeTextMap` => Model.DamlLfPrimType.TextMap
  }

  def jsValueToDamlLfDataType(value: JsValue): Model.DamlLfDataType =
    strField(value, propType, "DamlLfDefDataType") match {
      case `tagTypeRecord` =>
        val fields = arrayField(value, propFields, "DamlLfRecord")
        Model.DamlLfRecord(
          Model.DamlLfImmArraySeq(
            fields.map(f =>
              (
                nameField(f, propName, "DamlLfRecord"),
                jsValueToDamlLfType(anyField(f, propValue, "DamlLfRecord")),
              )
            ): _*
          )
        )
      case `tagTypeVariant` =>
        val constructors = arrayField(value, propFields, "DamlLfVariant")
        Model.DamlLfVariant(
          Model.DamlLfImmArraySeq(
            constructors.map(f =>
              (
                nameField(f, propName, "DamlLfVariant"),
                jsValueToDamlLfType(anyField(f, propValue, "DamlLfVariant")),
              )
            ): _*
          )
        )
      case `tagTypeEnum` =>
        val constructors = arrayField(value, propConstructors, "DamlLfEnum")
        Model.DamlLfEnum(Model.DamlLfImmArraySeq(constructors: _*).map(asName(_, "DamlLfEnum")))
      case t =>
        deserializationError(
          s"Can't read ${value.prettyPrint} as DamlLfDataType, unknown type '$t'"
        )
    }

  def jsValueToDamlLfDefDataType(value: JsValue): Model.DamlLfDefDataType = {
    val vars =
      arrayField(value, propVars, "DamlLfDefDataType").map(v => asName(v, "DamlLfDefDataType"))
    val dataType = jsValueToDamlLfDataType(anyField(value, propType, "DamlLfDefDataType"))

    Model.DamlLfDefDataType(Model.DamlLfImmArraySeq(vars: _*), dataType)
  }

  def jsValueToDamlLfIdentifier(value: JsValue): Model.DamlLfIdentifier =
    Model.DamlLfIdentifier(
      DamlLfRef.PackageId.assertFromString(strField(value, propPackage, "DamlLfIdentifier")),
      Model.DamlLfQualifiedName(
        Model.DamlLfDottedName.assertFromString(strField(value, propModule, "DamlLfIdentifier")),
        Model.DamlLfDottedName.assertFromString(strField(value, propName, "DamlLfIdentifier")),
      ),
    )

  // ------------------------------------------------------------------------------------------------------------------
  // Implicits that can be imported for .parseJson and .toJson functions
  // ------------------------------------------------------------------------------------------------------------------
  object JsonImplicits extends DefaultJsonProtocol {
    implicit object DamlLfTypeJsonFormat extends RootJsonFormat[Model.DamlLfType] {
      def write(v: Model.DamlLfType): JsValue = damlLfTypeToJsValue(v)
      def read(v: JsValue): Model.DamlLfType = jsValueToDamlLfType(v)
    }

    implicit object DamlLfDataTypeJsonFormat extends RootJsonFormat[Model.DamlLfDataType] {
      def write(v: Model.DamlLfDataType): JsValue = damlLfDataTypeToJsValue(v)
      def read(v: JsValue): Model.DamlLfDataType = jsValueToDamlLfDataType(v)
    }

    implicit object DamlLfDefDataTypeJsonFormat extends RootJsonFormat[Model.DamlLfDefDataType] {
      def write(v: Model.DamlLfDefDataType): JsValue = damlLfDefDataTypeToJsValue(v)
      def read(v: JsValue): Model.DamlLfDefDataType = jsValueToDamlLfDefDataType(v)
    }

    implicit object DamlLfIdentifierJsonFormat extends RootJsonFormat[Model.DamlLfIdentifier] {
      def write(v: Model.DamlLfIdentifier): JsValue = damlLfIdentifierToJsValue(v)
      def read(v: JsValue): Model.DamlLfIdentifier = jsValueToDamlLfIdentifier(v)
    }
  }
}
