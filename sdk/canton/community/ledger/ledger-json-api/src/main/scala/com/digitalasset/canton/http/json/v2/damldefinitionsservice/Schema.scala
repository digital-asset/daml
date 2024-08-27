// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2.damldefinitionsservice

import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.SerializableType.*
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

object Schema {
  type JsIdentifier = String

  val Empty: TypeSig = TypeSig(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)

  sealed trait SerializableType extends Product with Serializable

  sealed trait TemplateOrInterface extends Product with Serializable

  object TemplateOrInterface {
    final case class Template(tycon: JsIdentifier) extends TemplateOrInterface

    final case class Interface(tycon: JsIdentifier) extends TemplateOrInterface
  }

  object SerializableType {
    final case class Enum(tycon: JsIdentifier) extends SerializableType

    case object Int64 extends SerializableType

    final case class Numeric(scale: Int) extends SerializableType

    case object Text extends SerializableType

    case object Timestamp extends SerializableType

    case object Date extends SerializableType

    case object Party extends SerializableType

    case object Bool extends SerializableType

    case object Unit extends SerializableType

    final case class Record(tyCon: JsIdentifier, params: Seq[SerializableType])
        extends SerializableType

    final case class Variant(tyCon: JsIdentifier, params: Seq[SerializableType])
        extends SerializableType

    final case class ContractId(typeId: Option[TemplateOrInterface]) extends SerializableType

    final case class List(typeParam: SerializableType) extends SerializableType

    final case class Optional(typeParam: SerializableType) extends SerializableType

    final case class GenMap(k: SerializableType, v: SerializableType) extends SerializableType

    final case class Var(name: String) extends SerializableType
  }

  sealed trait DataType extends Product with Serializable

  final case class RecordSig(params: Seq[String], fields: Map[String, SerializableType])
      extends DataType

  final case class VariantSig(params: Seq[String], constructor: Map[String, SerializableType])
      extends DataType

  final case class EnumSig(constructor: Seq[String]) extends DataType

  final case class ChoiceSig(
      consuming: Boolean,
      argType: SerializableType,
      returnType: SerializableType,
  )

  final case class TemplateSig(
      key: Option[SerializableType],
      choices: Map[String, ChoiceSig],
      implements: Set[JsIdentifier],
  )
  final case class InterfaceSig(choices: Map[String, ChoiceSig], viewType: SerializableType)

  case class TypeSig(
      enumDefs: Map[JsIdentifier, EnumSig],
      variantDefs: Map[JsIdentifier, VariantSig],
      recordDefs: Map[JsIdentifier, RecordSig],
      templateDefs: Map[JsIdentifier, TemplateSig],
      interfaceDefs: Map[JsIdentifier, InterfaceSig],
  ) {

    val addEnumDefs: (JsIdentifier, EnumSig) => TypeSig =
      (id, defn) => copy(enumDefs = enumDefs.updated(id, defn))

    val addVariantDefs: (JsIdentifier, VariantSig) => TypeSig =
      (id, defn) => copy(variantDefs = variantDefs.updated(id, defn))

    val addRecordDefs: (JsIdentifier, RecordSig) => TypeSig =
      (id, defn) => copy(recordDefs = recordDefs.updated(id, defn))

    val addTemplateDefs: (JsIdentifier, TemplateSig) => TypeSig =
      (id, defn) => copy(templateDefs = templateDefs.updated(id, defn))

    val addInterfaceDefs: (JsIdentifier, InterfaceSig) => TypeSig =
      (id, defn) => copy(interfaceDefs = interfaceDefs.updated(id, defn))

    def merge(other: TypeSig): TypeSig = TypeSig(
      enumDefs ++ other.enumDefs,
      variantDefs ++ other.variantDefs,
      recordDefs ++ other.recordDefs,
      templateDefs ++ other.templateDefs,
      interfaceDefs ++ other.interfaceDefs,
    )
  }

  final case class AllTemplatesResponse(templates: Set[JsIdentifier])

  final case class ChoiceDefinition(
      consuming: Boolean,
      arguments: RecordSig,
      returnType: SerializableType,
  )

  final case class InterfaceDefinition(
      choices: Map[String, ChoiceDefinition],
      viewType: RecordSig,
  )

  final case class TemplateDefinition(
      arguments: RecordSig,
      key: Option[SerializableType],
      choices: Map[String, ChoiceDefinition],
      implements: Map[JsIdentifier, InterfaceDefinition],
      definitions: Map[JsIdentifier, DataType],
  )

  object Codecs {
    implicit val enumSigCodec: Codec[EnumSig] = deriveCodec
    implicit val variantSigCodec: Codec[VariantSig] = deriveCodec
    implicit val recordSigCodec: Codec[RecordSig] = deriveCodec

    implicit val choiceSigCodec: Codec[ChoiceSig] = deriveCodec
    implicit val templateSigCodec: Codec[TemplateSig] = deriveCodec
    implicit val interfaceSigCodec: Codec[InterfaceSig] = deriveCodec

    // TODO(#19671): For readability, represent Scala objects as simple JSON strings instead of nested JSON objects with empty body
    implicit val templateOrInterfaceCodec: Codec[TemplateOrInterface] = deriveCodec
    implicit val serializableTypeCodec: Codec[SerializableType] = deriveCodec
    implicit val enumCodec: Codec[Enum] = deriveCodec
    implicit val int64Codec: Codec[Int64.type] = deriveCodec
    implicit val numericCodec: Codec[Numeric] = deriveCodec
    implicit val textCodec: Codec[Text.type] = deriveCodec
    implicit val timestampCodec: Codec[Timestamp.type] = deriveCodec
    implicit val dateCodec: Codec[Date.type] = deriveCodec
    implicit val partyCodec: Codec[Party.type] = deriveCodec
    implicit val boolCodec: Codec[Bool.type] = deriveCodec
    implicit val unitCodec: Codec[SerializableType.Unit.type] = deriveCodec
    implicit val recordCodec: Codec[Record] = deriveCodec
    implicit val variantCodec: Codec[Variant] = deriveCodec
    implicit val contractIdCodec: Codec[ContractId] = deriveCodec
    implicit val listCodec: Codec[List] = deriveCodec
    implicit val optionalCodec: Codec[Optional] = deriveCodec
    implicit val genMapCodec: Codec[GenMap] = deriveCodec
    implicit val varCodec: Codec[Var] = deriveCodec
    implicit val typeSigCodec: Codec[TypeSig] = deriveCodec
    implicit val dataTypeCodec: Codec[DataType] = deriveCodec
    implicit val templateDefinitionCodec: Codec[TemplateDefinition] = deriveCodec
    implicit val choiceDefinitionCodec: Codec[ChoiceDefinition] = deriveCodec
    implicit val interfaceDefinitionCodec: Codec[InterfaceDefinition] = deriveCodec
    implicit val allTemplatesResponseCodec: Codec[AllTemplatesResponse] = deriveCodec
  }
}
