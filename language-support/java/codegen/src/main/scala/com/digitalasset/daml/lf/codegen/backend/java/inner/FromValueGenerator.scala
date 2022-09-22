// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{
  ContractCompanion,
  ValueDecoder,
  PrimitiveValueDecoders,
}
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.typesig._
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

private[inner] object FromValueGenerator extends StrictLogging {

  def generateDeprecatedFromValueForRecordLike(
      className: TypeName,
      typeParameters: IndexedSeq[String],
  ): MethodSpec = {
    logger.debug("Generating fromValue method")

    val converterParams = FromValueExtractorParameters
      .generate(typeParameters)
      .functionParameterSpecs

    val method = MethodSpec
      .methodBuilder("fromValue")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(className)
      .addTypeVariables(className.typeParameters)
      .addParameter(TypeName.get(classOf[javaapi.data.Value]), "value$")
      .addParameters(converterParams.asJava)
      .addException(classOf[IllegalArgumentException])
      .addAnnotation(classOf[Deprecated])
      .addJavadoc(
        "@deprecated since Daml $L; $L",
        "2.5.0",
        s"use {@code fromValue} that return ValueDecoder<?> instead",
      )

    val fromValueParams = CodeBlock.join(
      converterParams.map { param =>
        CodeBlock.of("$N::apply", param)
      }.asJava,
      ", ",
    )

    val classStaticAccessor = if (className.typeParameters.size > 0) {
      val typeParameterList = CodeBlock.join(
        className.typeParameters.asScala.map { param =>
          CodeBlock.of("$T", param)
        }.asJava,
        ", ",
      )
      CodeBlock.of("$T.<$L>", className.rawType, typeParameterList)
    } else CodeBlock.of("")

    method
      .addStatement(
        "return $LfromValue($L).decode($L)",
        classStaticAccessor,
        fromValueParams,
        "value$",
      )
      .build
  }

  def generateFromValueForRecordLike(
      fields: Fields,
      className: TypeName,
      typeParameters: IndexedSeq[String],
      methodName: String,
      recordValueExtractor: (String, String) => CodeBlock,
      packagePrefixes: Map[PackageId, String],
      isPublic: Boolean = true,
  ): MethodSpec = {
    logger.debug(s"Generating value decoder method $methodName")

    val converterParams = FromValueExtractorParameters
      .generate(typeParameters)
      .fromValueParameterSpecs

    val fromValueCode = CodeBlock
      .builder()
      .add(recordValueExtractor("value$", "recordValue$"))
      .addStatement(
        "$T record$$ = recordValue$$.asRecord().orElseThrow(() -> new IllegalArgumentException($S))",
        classOf[javaapi.data.DamlRecord],
        "Contracts must be constructed from Records",
      )
      .addStatement(
        "$T fields$$ = record$$.getFields()",
        ParameterizedTypeName
          .get(classOf[java.util.List[_]], classOf[javaapi.data.DamlRecord.Field]),
      )
      .addStatement("int numberOfFields = fields$$.size()")
      .beginControlFlow(s"if (numberOfFields != ${fields.size})")
      .addStatement(
        "throw new $T($S + numberOfFields)",
        classOf[IllegalArgumentException],
        s"Expected ${fields.size} arguments, got ",
      )
      .endControlFlow()

    fields.iterator.zip(accessors).foreach { case (FieldInfo(_, damlType, javaName, _), accessor) =>
      fromValueCode.addStatement(
        generateFieldExtractor(damlType, javaName, accessor, packagePrefixes)
      )
    }

    fromValueCode
      .addStatement(
        "return new $T($L)",
        className,
        generateArgumentList(fields.map(_.javaName)),
      )

    MethodSpec
      .methodBuilder(methodName)
      .addModifiers(if (isPublic) Modifier.PUBLIC else Modifier.PRIVATE, Modifier.STATIC)
      .returns(ParameterizedTypeName.get(ClassName.get(classOf[ValueDecoder[_]]), className))
      .addTypeVariables(className.typeParameters)
      .addParameters(converterParams.asJava)
      .addException(classOf[IllegalArgumentException])
      .beginControlFlow("return $L ->", "value$")
      .addCode(fromValueCode.build())
      // put empty string in endControlFlow in order to have semicolon
      .endControlFlow("")
      .build()
  }

  def generateContractCompanionValueDecoder(
      className: TypeName,
      typeParameters: IndexedSeq[String],
  ): MethodSpec = {
    logger.debug("Generating method for getting value decoder in template class")
    val converterParams = FromValueExtractorParameters
      .generate(typeParameters)
      .fromValueParameterSpecs

    MethodSpec
      .methodBuilder("fromValue")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(ParameterizedTypeName.get(ClassName.get(classOf[ValueDecoder[_]]), className))
      .addTypeVariables(className.typeParameters)
      .addParameters(converterParams.asJava)
      .addException(classOf[IllegalArgumentException])
      .addStatement("return $T.fromValue(COMPANION)", classOf[ContractCompanion[_, _, _]])
      .build()
  }

  private def accessors =
    Iterator.from(0).map(i => CodeBlock.of("fields$$.get($L).getValue()", Integer.valueOf(i)))

  def variantCheck(constructorName: String, inputVar: String, outputVar: String): CodeBlock = {
    CodeBlock
      .builder()
      .addStatement(
        "$T variant$$ = $L.asVariant().orElseThrow(() -> new IllegalArgumentException($S + $L.getClass().getName()))",
        classOf[javaapi.data.Variant],
        inputVar,
        s"Expected: Variant. Actual: ",
        inputVar,
      )
      .addStatement(
        "if (!$S.equals(variant$$.getConstructor())) throw new $T($S + variant$$.getConstructor())",
        constructorName,
        classOf[IllegalArgumentException],
        s"Invalid constructor. Expected: $constructorName. Actual: ",
      )
      .addStatement("$T $L = variant$$.getValue()", classOf[javaapi.data.Value], outputVar)
      .build()
  }

  def generateFieldExtractor(
      fieldType: Type,
      field: String,
      accessor: CodeBlock,
      packagePrefixes: Map[PackageId, String],
  ): CodeBlock =
    CodeBlock.of(
      "$T $L = $L",
      toJavaTypeName(fieldType, packagePrefixes),
      field,
      extractor(fieldType, field, accessor, newNameGenerator, packagePrefixes),
    )

  private[this] val extractors =
    Map[PrimType, String](
      (PrimTypeBool, "fromBool"),
      (PrimTypeInt64, "fromInt64"),
      (PrimTypeText, "fromText"),
      (PrimTypeTimestamp, "fromTimestamp"),
      (PrimTypeParty, "fromParty"),
      (PrimTypeUnit, "fromUnit"),
      (PrimTypeDate, "fromDate"),
    )

  // If [[typeName]] is defined in the primitive extractors map, create an extractor for it
  private def primitive(
      damlType: PrimType,
      apiType: TypeName,
      field: String,
      accessor: CodeBlock,
  ): Option[CodeBlock] =
    extractors
      .get(damlType)
      .map { extractor =>
        logger.debug(s"Generating primitive extractor for $field of type $apiType")
        CodeBlock.of(
          "$T.$L.decode($L)",
          classOf[PrimitiveValueDecoders],
          extractor,
          accessor,
        )
      }

  private def orElseThrow(typeName: TypeName, field: String) =
    CodeBlock.of(
      ".orElseThrow(() -> new IllegalArgumentException($S))",
      s"Expected $field to be of type $typeName",
    )

  /** Generates extractor for types that are not immediately covered by primitive extractors
    * @param damlType The type of the field being accessed
    * @param field The name of the field being accessed
    * @param accessor The [[CodeBlock]] that defines how to access the item in the first place
    * @param args An iterator providing argument names for nested calls without shadowing
    * @return A [[CodeBlock]] that defines the extractor for the whole composite type
    */
  private[inner] def extractor(
      damlType: Type,
      field: String,
      accessor: CodeBlock,
      args: Iterator[String],
      packagePrefixes: Map[PackageId, String],
  ): CodeBlock = {

    lazy val apiType = toAPITypeName(damlType)
    lazy val javaType = toJavaTypeName(damlType, packagePrefixes)
    logger.debug(s"Generating composite extractor for $field of type $javaType")

    damlType match {
      // Case #1: the type is actually a type parameter: we assume the calling code defines a
      // suitably named function that takes the result of accessing the underlying data (as
      // defined by the accessor
      // TODO: review aforementioned assumption
      case TypeVar(tvName) =>
        CodeBlock.of("fromValue$L.decode($L)", JavaEscaper.escapeString(tvName), accessor)

      case TypePrim(PrimTypeList, ImmArraySeq(param)) =>
        val optMapArg = args.next()
        val listMapArg = args.next()
        CodeBlock.of(
          """$L.asList()
            |    .map($L -> $L.toList($L ->
            |        $L
            |    ))
            |    $L
            |""".stripMargin,
          accessor,
          optMapArg,
          optMapArg,
          listMapArg,
          extractor(param, listMapArg, CodeBlock.of("$L", listMapArg), args, packagePrefixes),
          orElseThrow(apiType, field),
        )

      case TypePrim(PrimTypeOptional, ImmArraySeq(param)) =>
        val optOptArg = args.next()
        val valArg = args.next()
        CodeBlock.of(
          """$L.asOptional()
            |    .map($L -> $L.toOptional($L ->
            |        $L
            |    ))
            |    $L
          """.stripMargin,
          accessor,
          optOptArg,
          optOptArg,
          valArg,
          extractor(param, valArg, CodeBlock.of("$L", valArg), args, packagePrefixes),
          orElseThrow(apiType, field),
        )
      case TypePrim(PrimTypeContractId, ImmArraySeq(TypeVar(name))) =>
        CodeBlock.of(
          "fromValue$L.fromContractId($L.asContractId()$L.getValue())",
          JavaEscaper.escapeString(name),
          accessor,
          orElseThrow(apiType, field),
        )
      case TypePrim(PrimTypeContractId, ImmArraySeq(_)) =>
        CodeBlock.of(
          "new $T($L.asContractId()$L.getValue())",
          javaType,
          accessor,
          orElseThrow(apiType, field),
        )
      case TypePrim(PrimTypeTextMap, ImmArraySeq(param)) =>
        val optMapArg = args.next()
        val entryArg = args.next()
        CodeBlock.of(
          """$L.asTextMap()
            |    .map($L -> $L.toMap($L ->
            |        $L
            |    ))
            |    $L
          """.stripMargin,
          accessor,
          optMapArg,
          optMapArg,
          entryArg,
          extractor(param, entryArg, CodeBlock.of("$L", entryArg), args, packagePrefixes),
          orElseThrow(apiType, field),
        )

      case TypePrim(PrimTypeGenMap, ImmArraySeq(keyType, valueType)) =>
        val optMapArg = args.next()
        val entryArg = args.next()
        CodeBlock.of(
          """$L.asGenMap()
              |    .map($L -> $L.toMap(
              |        $L -> $L,
              |        $L -> $L
              |    ))
              |    $L
          """.stripMargin,
          accessor,
          optMapArg,
          optMapArg,
          entryArg,
          extractor(keyType, entryArg, CodeBlock.of("$L", entryArg), args, packagePrefixes),
          entryArg,
          extractor(valueType, entryArg, CodeBlock.of("$L", entryArg), args, packagePrefixes),
          orElseThrow(apiType, field),
        )

      case TypeNumeric(_) =>
        CodeBlock.of("$L.asNumeric()$L.getValue()", accessor, orElseThrow(apiType, field))

      case TypePrim(prim, _) =>
        primitive(prim, apiType, field, accessor).getOrElse(
          sys.error(s"Unhandled primitive type $prim")
        )

      case TypeCon(_, ImmArraySeq()) =>
        CodeBlock.of("$T.fromValue().decode($L)", javaType, accessor)

      case TypeCon(_, typeParameters) =>
        val (targs, valueDecoders) = typeParameters.map {
          case targ @ TypeVar(tvName) =>
            toJavaTypeName(targ, packagePrefixes) -> CodeBlock.of(
              "fromValue$L",
              JavaEscaper.escapeString(tvName),
            )
          case targ @ TypeCon(_, ImmArraySeq()) =>
            toJavaTypeName(targ, packagePrefixes) -> CodeBlock.of(
              "$T.fromValue()",
              toJavaTypeName(targ, packagePrefixes),
            )
          case targ =>
            val innerArg = args.next()
            toJavaTypeName(targ, packagePrefixes) -> CodeBlock.of(
              "$L -> $L",
              innerArg,
              extractor(targ, field, CodeBlock.of("$L", innerArg), args, packagePrefixes),
            )
        }.unzip

        val targsCode = CodeBlock.join(targs.map(CodeBlock.of("$L", _)).asJava, ", ")
        CodeBlock
          .builder()
          .add(CodeBlock.of("$T.<$L>fromValue(", javaType.rawType, targsCode))
          .add(CodeBlock.join(valueDecoders.asJava, ", "))
          .add(CodeBlock.of(").decode($L)", accessor))
          .build()
    }
  }
}
