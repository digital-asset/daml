// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.codegen.json.{JsonLfEncoder, JsonLfEncoders, JsonLfWriter}
import com.daml.lf.typesig.Type
import com.squareup.javapoet.{
  CodeBlock,
  ClassName,
  TypeName,
  MethodSpec,
  TypeVariableName,
  ParameterizedTypeName,
  ParameterSpec,
}
import java.io.{IOException, UncheckedIOException, StringWriter}
import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

private[inner] object ToJsonGenerator {

  private val encodersClass = classOf[JsonLfEncoders]
  private val fieldClass = classOf[JsonLfEncoders.Field]

  // Variants are encoded via a JsonLfEncoder.Field, which describes
  // how to encode this particular instance.
  // The definition of the Field is delegated out to the subclasses
  // which each override a `fieldForJsonEncoder` method.
  // For each type parameter to the variant, an encoder function
  // will need to be passed in.
  def forVariant(className: ClassName, typeParams: IndexedSeq[String]): Seq[MethodSpec] = {
    def abstractFieldForJsonEncoder = MethodSpec
      .methodBuilder("fieldForJsonEncoder")
      .addModifiers(Modifier.PROTECTED, Modifier.ABSTRACT)
      .addParameters(jsonEncoderParamsForTypeParams(typeParams))
      .returns(fieldClass)
      .build()

    def jsonEncoder = MethodSpec
      .methodBuilder("jsonEncoder")
      .addModifiers(Modifier.PUBLIC)
      .addParameters(jsonEncoderParamsForTypeParams(typeParams))
      .addStatement {
        val encoderFieldFunc =
          if (typeParams.isEmpty) CodeBlock.of("$T::fieldForJsonEncoder", className)
          else
            CodeBlock.of(
              "($T _x) -> _x.fieldForJsonEncoder($L)",
              className.parameterized(typeParams),
              jsonEncoderArgsForTypeParams(typeParams),
            )
        CodeBlock.of("return $T.variant($L).apply(this)", encodersClass, encoderFieldFunc)
      }
      .returns(classOf[JsonLfEncoder])
      .build()
    Seq(abstractFieldForJsonEncoder, jsonEncoder) ++ toJson(typeParams).toList
  }

  // A different code-gen strategy is employed for "simple" variants vs "record" variants, i.e.
  // data Foo = SimpleFoo Int | RecordFoo with x: Int, y: Text

  // In this case, the encoder is simply the one for the single data arg.
  def forVariantSimple(
      constructorName: String,
      typeParams: IndexedSeq[String],
      fieldName: String,
      damlType: Type,
  )(implicit
      packagePrefixes: PackagePrefixes
  ): (Seq[MethodSpec], Seq[(ClassName, String)]) = {
    val encoder = CodeBlock.of("apply($L, $L)", encoderOf(damlType), fieldName)
    val methods = Seq(fieldForJsonEncoder(constructorName, typeParams, encoder))
    val staticImports = Seq((ClassName.get(encodersClass), "apply"))
    (methods, staticImports)
  }

  // Encoding a record is more involved, so we pull that out into its own method,
  // and reference that method when building the Field encoder.
  def forVariantRecord(constructorName: String, fields: Fields, typeParams: IndexedSeq[String])(
      implicit packagePrefixes: PackagePrefixes
  ): (Seq[MethodSpec], Seq[(ClassName, String)]) = {
    val recordEncoderMethodName = s"jsonEncoder${constructorName}"
    val (recordJsonEncoder, staticImports) =
      jsonEncoderForRecordLike(recordEncoderMethodName, Modifier.PRIVATE, typeParams, fields)
    val fieldEncoder = CodeBlock.of(
      "this.$L($L)",
      recordEncoderMethodName,
      jsonEncoderArgsForTypeParams(typeParams),
    )
    val methods = Seq(
      recordJsonEncoder,
      fieldForJsonEncoder(constructorName, typeParams, fieldEncoder),
    )
    (methods, staticImports)
  }

  def forEnum(className: ClassName): Seq[MethodSpec] = {
    val getConstructor = MethodSpec
      .methodBuilder("getConstructor")
      .addModifiers(Modifier.PUBLIC)
      .addStatement("return toValue().getConstructor()", className)
      .returns(classOf[String])
      .build()

    val jsonEncoder = MethodSpec
      .methodBuilder("jsonEncoder")
      .addModifiers(Modifier.PUBLIC)
      .addStatement(
        "return $T.enumeration(($T e$$) -> e$$.getConstructor()).apply(this)",
        encodersClass,
        className,
      )
      .returns(classOf[JsonLfEncoder])
      .build()

    Seq(getConstructor, jsonEncoder)
  }

  def forRecordLike(fields: Fields, typeParams: IndexedSeq[String])(implicit
      packagePrefixes: PackagePrefixes
  ): (Seq[MethodSpec], Seq[(ClassName, String)]) = {
    val (jsonEncoder, staticImports) =
      jsonEncoderForRecordLike("jsonEncoder", Modifier.PUBLIC, typeParams, fields)
    val methods = Seq(jsonEncoder) ++ toJson(typeParams).toList
    (methods, staticImports)
  }

  // When a type has type parameters (generic classes), we need to tell
  // the encoder how to encode that type argument,
  // e.g. if encoding a List<T>, we need to tell it how to encode a T.
  // This is done by passing functions for each type parameter as arguments to the encoding methods.
  // These functions can create a JsonLfEncoder for each value of the relevant type.
  // These arguments are given a name based on the type param, defined here.
  private def makeEncoderParamName(t: String): String = s"makeEncoder_${t}"

  // Argument names, used when calling method.
  private def jsonEncoderArgsForTypeParams(typeParams: IndexedSeq[String]) =
    CodeBlock.join(typeParams.map(t => CodeBlock.of(makeEncoderParamName(t))).asJava, ",$W")

  // ParameterSpec's, used when defining method.
  private def jsonEncoderParamsForTypeParams(
      typeParams: IndexedSeq[String]
  ): java.util.List[ParameterSpec] = {
    def makeEncoderParamType(t: String): TypeName =
      ParameterizedTypeName.get(
        ClassName.get(classOf[java.util.function.Function[_, _]]),
        TypeVariableName.get(t),
        ClassName.get(classOf[JsonLfEncoder]),
      )

    typeParams.map { t =>
      ParameterSpec
        .builder(makeEncoderParamType(t), makeEncoderParamName(t))
        .build()
    }.asJava
  }

  private def toJson(typeParams: IndexedSeq[String]): Option[MethodSpec] =
    if (typeParams.isEmpty)
      // Types without parameters inherit from DefinedDataType and gets toJson from there.
      None
    else
      // These types do not inherit from DefinedDataType, so need to have toJson added.
      // The signature of toJson is different as it will need to accept an argument with an encoder
      // for each type parameter.
      Some(
        MethodSpec
          .methodBuilder("toJson")
          .addModifiers(Modifier.PUBLIC)
          .addParameters(jsonEncoderParamsForTypeParams(typeParams))
          .addStatement("var w = new $T()", classOf[StringWriter])
          .beginControlFlow("try")
          .addStatement(
            "this.jsonEncoder($L).encode(new $T(w))",
            jsonEncoderArgsForTypeParams(typeParams),
            classOf[JsonLfWriter],
          )
          .nextControlFlow("catch ($T e)", classOf[IOException])
          .addComment("Not expected with StringWriter")
          .addStatement("throw new $T(e)", classOf[UncheckedIOException])
          .endControlFlow()
          .addStatement("return w.toString()")
          .returns(ClassName.get(classOf[String]))
          .build()
      )

  private def jsonEncoderForRecordLike(
      methodName: String,
      modifier: Modifier,
      typeParams: IndexedSeq[String],
      fields: Fields,
  )(implicit
      packagePrefixes: PackagePrefixes
  ): (MethodSpec, Seq[(ClassName, String)]) = {
    val encoderFields = fields.map { f =>
      val encoder = CodeBlock.of("apply($L, $L)", encoderOf(f.damlType), f.javaName)
      CodeBlock.of(
        "$T.of($S, $L)",
        fieldClass,
        f.javaName,
        encoder,
      )
    }
    val jsonEncoder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(modifier)
      .addParameters(jsonEncoderParamsForTypeParams(typeParams))
      .returns(classOf[JsonLfEncoder])
      .addStatement(
        "return $T.record($Z$L)",
        encodersClass,
        CodeBlock.join(encoderFields.asJava, ",$Z"),
      )
      .build()

    val staticImports = Seq((ClassName.get(encodersClass), "apply"))
    (jsonEncoder, staticImports)
  }

  private def fieldForJsonEncoder(
      constructorName: String,
      typeParams: IndexedSeq[String],
      encoder: CodeBlock,
  ) = MethodSpec
    .methodBuilder("fieldForJsonEncoder")
    .addModifiers(Modifier.PROTECTED)
    .addParameters(jsonEncoderParamsForTypeParams(typeParams))
    .addAnnotation(classOf[Override])
    .addStatement("return $T.of($S,$W$L)", fieldClass, constructorName, encoder)
    .returns(fieldClass)
    .build()

  private def encoderOf(
      damlType: Type,
      nesting: Int = 0, // Used to avoid clashing argument identifiers in nested encoder definitions
  )(implicit packagePrefixes: PackagePrefixes): CodeBlock = {
    import com.daml.lf.typesig._

    def typeEncoders(types: Iterable[Type]): CodeBlock =
      CodeBlock.join(types.map(t => encoderOf(t, nesting + 1)).asJava, ", ")

    damlType match {
      case TypeCon(TypeConName(ident), IndexedSeq()) =>
        CodeBlock.of("$T::jsonEncoder", guessClass(ident))
      case TypeCon(TypeConName(_), typeParams) =>
        // We are introducing identifiers into the namespace, so try to make them unique.
        val argName = CodeBlock.of("_x$L", nesting)
        CodeBlock.of("$L -> $L.jsonEncoder($L)", argName, argName, typeEncoders(typeParams))
      case TypePrim(PrimTypeUnit, _) => CodeBlock.of("$T::unit", encodersClass)
      case TypePrim(PrimTypeBool, _) => CodeBlock.of("$T::bool", encodersClass)
      case TypePrim(PrimTypeInt64, _) => CodeBlock.of("$T::int64", encodersClass)
      case TypeNumeric(_) => CodeBlock.of("$T::numeric", encodersClass)
      case TypePrim(PrimTypeText, _) => CodeBlock.of("$T::text", encodersClass)
      case TypePrim(PrimTypeDate, _) => CodeBlock.of("$T::date", encodersClass)
      case TypePrim(PrimTypeTimestamp, _) => CodeBlock.of("$T::timestamp", encodersClass)
      case TypePrim(PrimTypeParty, _) => CodeBlock.of("$T::party", encodersClass)
      case TypePrim(PrimTypeContractId, _) => CodeBlock.of("$T::contractId", encodersClass)
      case TypePrim(PrimTypeList, Seq(typ)) =>
        CodeBlock.of("$T.list($L)", encodersClass, encoderOf(typ, nesting + 1))
      case TypePrim(PrimTypeOptional, Seq(typ)) =>
        def buildNestedOptionals(b: CodeBlock.Builder, typ: Type): CodeBlock.Builder = typ match {
          case TypePrim(PrimTypeOptional, Seq(innerType)) =>
            buildNestedOptionals(b.add("$T.optionalNested(", encodersClass), innerType).add(")")
          case _ =>
            b.add("$T.optional($L)", encodersClass, encoderOf(typ, nesting + 1))
        }
        buildNestedOptionals(CodeBlock.builder(), typ).build()
      case TypePrim(PrimTypeTextMap, Seq(valType)) =>
        CodeBlock.of("$T.textMap($L)", encodersClass, encoderOf(valType, nesting + 1))
      case TypePrim(PrimTypeGenMap, Seq(keyType, valType)) =>
        CodeBlock.of(
          "$T.genMap($L, $L)",
          encodersClass,
          encoderOf(keyType, nesting + 1),
          encoderOf(valType, nesting + 1),
        )
      case TypeVar(t) => CodeBlock.of(makeEncoderParamName(t))
      case _ => throw new IllegalArgumentException(s"Invalid Daml datatype: $damlType")
    }
  }
}
