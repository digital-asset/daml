// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.codegen.json.{JsonLfReader, JsonLfDecoder, JsonLfDecoders}
import com.typesafe.scalalogging.StrictLogging
import javax.lang.model.element.Modifier
import com.squareup.javapoet.{
  CodeBlock,
  ClassName,
  MethodSpec,
  ParameterSpec,
  ParameterizedTypeName,
  TypeName,
  TypeVariableName,
}
import scala.jdk.CollectionConverters._

private[inner] object FromJsonGenerator extends StrictLogging {
  private def decodeClass = ClassName.get(classOf[JsonLfDecoders])

  // JsonLfDecoder<T>
  private def decoderTypeName(t: TypeName) =
    ParameterizedTypeName.get(ClassName.get(classOf[JsonLfDecoder[_]]), t)

  private def decodeTypeParamName(t: String): String = s"decode$t"
  private def decoderForTagName(t: String): String = s"jsonDecoder$t"

  private def jsonDecoderParamsForTypeParams(
      typeParams: IndexedSeq[String]
  ): java.util.List[ParameterSpec] =
    typeParams.map { t =>
      ParameterSpec
        .builder(decoderTypeName(TypeVariableName.get(t)), decodeTypeParamName(t))
        .build()
    }.asJava

  def forRecordLike(fields: Fields, className: ClassName, typeParams: IndexedSeq[String])(implicit
      packagePrefixes: PackagePrefixes
  ): Seq[MethodSpec] = {
    Seq(
      forRecordLike(
        "jsonDecoder",
        Seq(Modifier.PUBLIC, Modifier.STATIC),
        fields,
        className,
        typeParams,
      ),
      fromJsonString(className, typeParams),
    )
  }

  private def forRecordLike(
      methodName: String,
      modifiers: Seq[Modifier],
      fields: Fields,
      className: ClassName,
      typeParams: IndexedSeq[String],
  )(implicit packagePrefixes: PackagePrefixes): MethodSpec = {
    val typeName = className.parameterized(typeParams)

    val argNames = {
      val names = fields.map(f => CodeBlock.of("$S", f.javaName))
      CodeBlock.of("$T.asList($L)", classOf[java.util.Arrays], CodeBlock.join(names.asJava, ", "))
    }

    val argsByName = {
      val block = CodeBlock
        .builder()
        .beginControlFlow("name ->")
        .beginControlFlow("switch (name)")
      fields.zipWithIndex.foreach { case (f, i) =>
        block.addStatement(
          "case $S: return $T.at($L, $L)",
          f.javaName,
          decodeClass.nestedClass("JavaArg"),
          i,
          jsonDecoderForType(f.damlType),
        )
      }
      block
        .addStatement("default: return null")
        .endControlFlow() // end switch
        .endControlFlow() // end lambda
        .build()
    }

    val constr = {
      val args =
        (0 until fields.size).map(CodeBlock.of("$T.cast(args[$L])", decodeClass, _))
      CodeBlock.of("(Object[] args) -> new $T($L)", typeName, CodeBlock.join(args.asJava, ", "))
    }

    MethodSpec
      .methodBuilder(methodName)
      .addModifiers(modifiers: _*)
      .addTypeVariables(typeParams.map(TypeVariableName.get).asJava)
      .addParameters(jsonDecoderParamsForTypeParams(typeParams))
      .returns(decoderTypeName(typeName))
      .addStatement(
        "return $T.record($L, $L, $L)",
        decodeClass,
        argNames,
        argsByName.toString(),
        constr,
      )
      .build()
  }

  private def fromJsonString(
      className: ClassName,
      typeParams: IndexedSeq[String],
  ): MethodSpec =
    MethodSpec
      .methodBuilder("fromJson")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .addTypeVariables(typeParams.map(TypeVariableName.get).asJava)
      .addParameter(classOf[String], "json")
      .addParameters(jsonDecoderParamsForTypeParams(typeParams))
      .returns(className.parameterized(typeParams))
      .addException(classOf[JsonLfDecoder.Error])
      .addStatement(
        "return jsonDecoder($L).decode(new $T(json))",
        CodeBlock.join(typeParams.map(t => CodeBlock.of(decodeTypeParamName(t))).asJava, ", "),
        classOf[JsonLfReader],
      )
      .build()

  def forVariant(
      className: ClassName,
      typeParams: IndexedSeq[String],
      fields: Fields,
  ): Seq[MethodSpec] = {
    val typeName = className.parameterized(typeParams)

    val tagNames = CodeBlock.of(
      "$T.asList($L)",
      classOf[java.util.Arrays],
      CodeBlock.join(fields.map(f => CodeBlock.of("$S", f.javaName)).asJava, ", "),
    )
    val variantsByTag = {
      val block = CodeBlock
        .builder()
        .beginControlFlow("name ->")
        .beginControlFlow("switch (name)")
      fields.foreach { f =>
        block.addStatement(
          "case $S: return $L($L)",
          f.damlName,
          decoderForTagName(f.damlName),
          CodeBlock.join(typeParams.map(t => CodeBlock.of(decodeTypeParamName(t))).asJava, ", "),
        )
      }
      block
        .addStatement("default: return null")
        .endControlFlow() // end switch
        .endControlFlow() // end lambda
        .build()
    }

    val jsonDecoder = MethodSpec
      .methodBuilder("jsonDecoder")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .addTypeVariables(typeParams.map(TypeVariableName.get).asJava)
      .addParameters(jsonDecoderParamsForTypeParams(typeParams))
      .returns(decoderTypeName(typeName))
      .addStatement("return $T.variant($L, $L)", decodeClass, tagNames, variantsByTag.toString())
      .build()

    Seq(jsonDecoder, fromJsonString(className, typeParams))
  }

  def forVariantRecord(
      tag: String,
      fields: Fields,
      className: ClassName,
      typeParams: IndexedSeq[String],
  )(implicit
      packagePrefixes: PackagePrefixes
  ) =
    forRecordLike(
      decoderForTagName(tag),
      Seq(Modifier.PRIVATE, Modifier.STATIC),
      fields,
      className,
      typeParams,
    )

  def forVariantSimple(typeName: TypeName, typeParams: IndexedSeq[String], field: FieldInfo)(
      implicit packagePrefixes: PackagePrefixes
  ) =
    MethodSpec
      .methodBuilder(decoderForTagName(field.damlName))
      .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
      .addTypeVariables(typeParams.map(TypeVariableName.get).asJava)
      .addParameters(jsonDecoderParamsForTypeParams(typeParams))
      .returns(decoderTypeName(typeName))
      .addStatement(
        "return r -> new $T($L.decode(r))",
        typeName,
        jsonDecoderForType(field.damlType),
      )
      .build()

  def forEnum(className: ClassName, damlNameToEnumMap: String): Seq[MethodSpec] = {
    val jsonDecoder = MethodSpec
      .methodBuilder("jsonDecoder")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(decoderTypeName(className))
      .addStatement("return $T.enumeration($L)", decodeClass, damlNameToEnumMap)
      .build()

    Seq(jsonDecoder, fromJsonString(className, IndexedSeq.empty[String]))
  }

  import com.daml.lf.typesig.Type
  private def jsonDecoderForType(
      damlType: Type
  )(implicit packagePrefixes: PackagePrefixes): CodeBlock = {
    import com.daml.lf.typesig._
    import com.daml.lf.data.ImmArray.ImmArraySeq
    import com.daml.ledger.javaapi.data.codegen.ContractId

    def typeReaders(types: Iterable[Type]): CodeBlock =
      CodeBlock.join(types.map(jsonDecoderForType).asJava, ", ")

    damlType match {
      case TypeCon(TypeConName(ident), typeParams) =>
        CodeBlock.of("$T.jsonDecoder($L)", guessClass(ident), typeReaders(typeParams))
      case TypePrim(PrimTypeBool, _) => CodeBlock.of("$T.bool", decodeClass)
      case TypePrim(PrimTypeInt64, _) => CodeBlock.of("$T.int64", decodeClass)
      case TypeNumeric(scale) => CodeBlock.of("$T.numeric($L)", decodeClass, scale)
      case TypePrim(PrimTypeText, _) => CodeBlock.of("$T.text", decodeClass)
      case TypePrim(PrimTypeDate, _) => CodeBlock.of("$T.date", decodeClass)
      case TypePrim(PrimTypeTimestamp, _) => CodeBlock.of("$T.timestamp", decodeClass)
      case TypePrim(PrimTypeParty, _) => CodeBlock.of("$T.party", decodeClass)
      case TypePrim(PrimTypeContractId, ImmArraySeq(templateType)) =>
        val contractIdType = toJavaTypeName(templateType) match {
          case templateClass: ClassName => templateClass.nestedClass("ContractId")
          case typeVariableName: TypeVariableName =>
            ParameterizedTypeName.get(ClassName.get(classOf[ContractId[_]]), typeVariableName)
          case unexpected => sys.error(s"Unexpected type [$unexpected] for Daml type [$damlType]")
        }
        CodeBlock.of("$T.contractId($T::new)", decodeClass, contractIdType)
      case TypePrim(PrimTypeList, typeParams) =>
        CodeBlock.of("$T.list($L)", decodeClass, typeReaders(typeParams))
      case TypePrim(PrimTypeOptional, Seq(typeParam)) =>
        def buildNestedOptionals(b: CodeBlock.Builder, typ: Type): CodeBlock.Builder = typ match {
          case TypePrim(PrimTypeOptional, Seq(innerType)) =>
            buildNestedOptionals(b.add("$T.optionalNested(", decodeClass), innerType).add(")")
          case _ =>
            b.add("$T.optional($L)", decodeClass, typeReaders(Seq(typ)))
        }
        buildNestedOptionals(CodeBlock.builder(), typeParam).build()
      case TypePrim(PrimTypeTextMap, typeParams) =>
        CodeBlock.of("$T.textMap($L)", decodeClass, typeReaders(typeParams))
      case TypePrim(PrimTypeGenMap, typeParams) =>
        CodeBlock.of("$T.genMap($L)", decodeClass, typeReaders(typeParams))
      case TypePrim(PrimTypeUnit, _) => CodeBlock.of("$T.unit", decodeClass)
      case TypeVar(name) => CodeBlock.of(decodeTypeParamName(name))
      case _ => throw new IllegalArgumentException(s"Invalid Daml datatype: $damlType")
    }
  }
}
