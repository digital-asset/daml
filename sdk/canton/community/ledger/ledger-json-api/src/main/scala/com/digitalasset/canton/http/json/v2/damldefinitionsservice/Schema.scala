// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2.damldefinitionsservice

import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.Numeric.Scale
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.TypeSig.*
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder, KeyDecoder, KeyEncoder}
import sttp.tapir.Schema as TapirSchema

import scala.collection.immutable.VectorMap

object Schema {
  val Empty: TypeSig = TypeSig(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)

  final case class TypeSig(
                            enumDefs: Map[Ref.TypeConId, EnumSig],
                            variantDefs: Map[Ref.TypeConId, VariantSig],
                            recordDefs: Map[Ref.TypeConId, RecordSig],
                            templateDefs: Map[Ref.TypeConId, TemplateSig],
                            interfaceDefs: Map[Ref.TypeConId, InterfaceSig],
  ) {

    val addEnumDefs: (Ref.TypeConId, EnumSig) => TypeSig =
      (id, defn) => copy(enumDefs = enumDefs.updated(id, defn))

    val addVariantDefs: (Ref.TypeConId, VariantSig) => TypeSig =
      (id, defn) => copy(variantDefs = variantDefs.updated(id, defn))

    val addRecordDefs: (Ref.TypeConId, RecordSig) => TypeSig =
      (id, defn) => copy(recordDefs = recordDefs.updated(id, defn))

    val addTemplateDefs: (Ref.TypeConId, TemplateSig) => TypeSig =
      (id, defn) => copy(templateDefs = templateDefs.updated(id, defn))

    val addInterfaceDefs: (Ref.TypeConId, InterfaceSig) => TypeSig =
      (id, defn) => copy(interfaceDefs = interfaceDefs.updated(id, defn))

    def merge(other: TypeSig): TypeSig = TypeSig(
      enumDefs ++ other.enumDefs,
      variantDefs ++ other.variantDefs,
      recordDefs ++ other.recordDefs,
      templateDefs ++ other.templateDefs,
      interfaceDefs ++ other.interfaceDefs,
    )
  }

  final case class AllTemplatesResponse(templates: Set[Ref.TypeConId])

  final case class ChoiceDefinition(
      consuming: Boolean,
      arguments: RecordSig,
      returnType: SerializableType,
  )

  final case class InterfaceDefinition(
      choices: Map[Ref.Name, ChoiceDefinition],
      viewType: RecordSig,
  )

  final case class TemplateDefinition(
                                       arguments: RecordSig,
                                       key: Option[SerializableType],
                                       choices: Map[Ref.Name, ChoiceDefinition],
                                       implements: Map[Ref.TypeConId, InterfaceDefinition],
                                       definitions: Map[Ref.TypeConId, DataTypeSig],
  )

  // See https://github.com/typelevel/doobie/issues/1513
  object Codecs {
    import SerializableType.*

    lazy implicit val nameKeyDecoder: KeyDecoder[Ref.Name] =
      KeyDecoder.instance(Ref.Name.fromString(_).toOption)

    lazy implicit val nameKeyEncoder: KeyEncoder[Ref.Name] = KeyEncoder.instance(identity)

    lazy implicit val nameCodec: Codec[Ref.Name] =
      Codec.from(
        Decoder.decodeString.emap(Ref.Name.fromString),
        Encoder.encodeString.contramap[Ref.Name](_.toString),
      )

    lazy implicit val typeConIdKeyDecoder: KeyDecoder[Ref.TypeConId] =
      KeyDecoder.instance(Ref.TypeConId.fromString(_).toOption)

    lazy implicit val typeConIdKeyEncoder: KeyEncoder[Ref.TypeConId] =
      KeyEncoder.instance(_.toString())

    lazy implicit val typeConIdCodec: Codec[Ref.TypeConId] = Codec.from(
      Decoder.decodeString.emap(Ref.TypeConId.fromString),
      Encoder.encodeString.contramap[Ref.TypeConId](_.toString()),
    )

    lazy implicit val scaleCodec: Codec[data.Numeric.Scale.Scale] =
      Codec.from(
        Decoder.decodeInt.emap(Scale.fromInt),
        Encoder.encodeInt.contramap(_.toInt),
      )
    lazy implicit val enumSigCodec: Codec[EnumSig] = deriveCodec
    lazy implicit val variantSigCodec: Codec[VariantSig] = deriveCodec
    lazy implicit val recordSigCodec: Codec[RecordSig] = deriveCodec

    lazy implicit val choiceSigCodec: Codec[ChoiceSig] = deriveCodec
    lazy implicit val templateSigCodec: Codec[TemplateSig] = deriveCodec
    lazy implicit val interfaceSigCodec: Codec[InterfaceSig] = deriveCodec

    // TODO(#21695): For readability, represent Scala objects as simple JSON strings instead of nested JSON objects with empty body
    lazy implicit val templateOrInterfaceCodec: Codec[TemplateOrInterface] = deriveCodec
    lazy implicit val serializableTypeCodec: Codec[SerializableType] = deriveCodec
    lazy implicit val enumCodec: Codec[Enum] = deriveCodec
    lazy implicit val int64Codec: Codec[Int64.type] = deriveCodec
    lazy implicit val numericCodec: Codec[Numeric] = deriveCodec
    lazy implicit val textCodec: Codec[Text.type] = deriveCodec
    lazy implicit val timestampCodec: Codec[Timestamp.type] = deriveCodec
    lazy implicit val dateCodec: Codec[Date.type] = deriveCodec
    lazy implicit val partyCodec: Codec[Party.type] = deriveCodec
    lazy implicit val boolCodec: Codec[Bool.type] = deriveCodec
    lazy implicit val unitCodec: Codec[SerializableType.Unit.type] = deriveCodec
    lazy implicit val recordCodec: Codec[Record] = deriveCodec
    lazy implicit val variantCodec: Codec[Variant] = deriveCodec
    lazy implicit val contractIdCodec: Codec[ContractId] = deriveCodec
    lazy implicit val listCodec: Codec[List] = deriveCodec
    lazy implicit val optionalCodec: Codec[Optional] = deriveCodec
    lazy implicit val genMapCodec: Codec[GenMap] = deriveCodec
    lazy implicit val varCodec: Codec[Var] = deriveCodec
    lazy implicit val typeSigCodec: Codec[TypeSig] = deriveCodec
    lazy implicit val dataTypeCodec: Codec[DataTypeSig] = deriveCodec
    lazy implicit val templateDefinitionCodec: Codec[TemplateDefinition] = deriveCodec
    lazy implicit val choiceDefinitionCodec: Codec[ChoiceDefinition] = deriveCodec
    lazy implicit val interfaceDefinitionCodec: Codec[InterfaceDefinition] = deriveCodec
    lazy implicit val allTemplatesResponseCodec: Codec[AllTemplatesResponse] = deriveCodec

    // The semi-auto derivation tree for Tapir is outlined below
    // since Tapir cannot properly use auto-derivation for deriving hierarchies including VectorMap and
    // Maps with keys other than String
    implicit def vectorMapSchema[K, V: TapirSchema]: TapirSchema[VectorMap[K, V]] =
      sttp.tapir.Schema.schemaForMap[K, V](_.toString).map(s => Some(VectorMap.from(s)))(_.toMap)

    implicit def genericKeyMapSchema[K, V: TapirSchema]: TapirSchema[Map[K, V]] =
      sttp.tapir.Schema.schemaForMap[K, V](_.toString)

    lazy implicit val nameTapirCodec: sttp.tapir.Schema[Ref.Name] =
      sttp.tapir.Schema.schemaForString
        .map(str => Ref.Name.fromString(str).toOption)(_.toString)
    lazy implicit val typeConIdTapirCodec: sttp.tapir.Schema[Ref.TypeConId] =
      sttp.tapir.Schema.schemaForString
        .map(str => Some(Ref.TypeConId.assertFromString(str)))(_.toString())
        .format("<package-id>:<module-name>:<entity-name>")

    lazy implicit val scaleTapirSchema: TapirSchema[Scale] =
      TapirSchema.schemaForInt.map(Scale.fromInt(_).toOption)(_.toInt)
    lazy implicit val serializableTypeTapirSchema: TapirSchema[SerializableType] =
      TapirSchema.derived
    lazy implicit val enumSigTapirSchema: TapirSchema[EnumSig] = TapirSchema.derived
    lazy implicit val templateOrInterfaceTapirSchema: TapirSchema[TemplateOrInterface] =
      TapirSchema.derived
    lazy implicit val variantSigTapirSchema: TapirSchema[VariantSig] = TapirSchema.derived
    lazy implicit val recordSigTapirSchema: TapirSchema[RecordSig] = TapirSchema.derived
    lazy implicit val choiceSigTapirSchema: TapirSchema[ChoiceSig] = TapirSchema.derived
    lazy implicit val templateSigTapirSchema: TapirSchema[TemplateSig] = TapirSchema.derived
    lazy implicit val interfaceSigTapirSchema: TapirSchema[InterfaceSig] = TapirSchema.derived
    lazy implicit val dataTypeTapirSchema: TapirSchema[DataTypeSig] = TapirSchema.derived
    lazy implicit val choiceDefinitionTapirSchema: TapirSchema[ChoiceDefinition] =
      TapirSchema.derived
    lazy implicit val typeSigTapirSchema: TapirSchema[TypeSig] = TapirSchema.derived
    lazy implicit val interfaceDefinitionTapirSchema: TapirSchema[InterfaceDefinition] =
      TapirSchema.derived
    lazy implicit val templateDefinitionTapirSchema: TapirSchema[TemplateDefinition] =
      TapirSchema.derived
    lazy implicit val allTemplatesResponseTapirSchema: TapirSchema[AllTemplatesResponse] =
      TapirSchema.derived
  }
}
