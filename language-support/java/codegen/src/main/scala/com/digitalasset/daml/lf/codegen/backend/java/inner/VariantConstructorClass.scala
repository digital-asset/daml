// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.Value
import com.daml.ledger.javaapi.data.codegen.FromValue
import com.daml.lf.codegen.backend.java.{JavaEscaper, ObjectMethods}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.typesig.{Type, TypeVar}
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

object VariantConstructorClass extends StrictLogging {

  def generate(
      packageId: PackageId,
      variant: TypeName,
      typeArgs: IndexedSeq[String],
      constructorName: String,
      javaName: String,
      body: Type,
      packagePrefixes: Map[PackageId, String],
  ): TypeSpec = {
    TrackLineage.of("variant constructor", constructorName) {
      logger.info("Start")

      val className = ClassName.bestGuess(javaName).parameterized(typeArgs)
      val javaType = toJavaTypeName(body, packagePrefixes)
      val variantFieldName = lowerCaseFieldName(body match {
        case TypeVar(typeArg) =>
          JavaEscaper.escapeString(typeArg)
        case _ =>
          javaType.rawType.simpleName
      })

      val constructor = ConstructorGenerator.generateConstructor(
        IndexedSeq(FieldInfo("body", body, variantFieldName, javaType))
      )

      val conversionMethods = distinctTypeVars(body, typeArgs).flatMap { params =>
        List(
          toValue(constructorName, params, body, variantFieldName, packagePrefixes),
          deprecatedFromValue(params, className),
          fromValue(constructorName, params, className, body, packagePrefixes),
        )
      }

      val typeSpec =
        TypeSpec
          .classBuilder(javaName)
          .addModifiers(Modifier.PUBLIC)
          .addTypeVariables(typeArgs.map(TypeVariableName.get).asJava)
          .superclass(variant)
          .addField(javaType, variantFieldName, Modifier.PUBLIC, Modifier.FINAL)
          .addField(createPackageIdField(packageId))
          .addMethod(constructor)
          .addMethods(conversionMethods.asJava)
          .addMethods(ObjectMethods(className.rawType, typeArgs, Vector(variantFieldName)).asJava)
          .build()

      logger.info("End")
      typeSpec
    }
  }

  private def toValue(
      constructor: String,
      typeArgs: IndexedSeq[String],
      body: Type,
      fieldName: String,
      packagePrefixes: Map[PackageId, String],
  ) = {
    val extractorParameters = ToValueExtractorParameters.generate(typeArgs)

    MethodSpec
      .methodBuilder("toValue")
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[data.Variant])
      .addParameters(extractorParameters.asJava)
      .addStatement(
        "return new $T($S, $L)",
        classOf[data.Variant],
        constructor,
        ToValueGenerator
          .generateToValueConverter(
            body,
            CodeBlock.of("this.$L", fieldName),
            newNameGenerator,
            packagePrefixes,
          ),
      )
      .build()
  }

  private def deprecatedFromValue(
      typeParameters: IndexedSeq[String],
      className: TypeName,
  ) = {
    val valueParam = ParameterSpec.builder(classOf[Value], "value$").build()

    val converterParams =
      FromValueExtractorParameters.generate(typeParameters).functionParameterSpecs
    val method = MethodSpec
      .methodBuilder("fromValue")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .addTypeVariables(className.typeParameters)
      .returns(className)
      .addException(classOf[IllegalArgumentException])
      .addParameter(valueParam)
      .addParameters(converterParams.asJava)
      .addAnnotation(classOf[Deprecated])
      .addJavadoc(
        "@deprecated since Daml $L; $L",
        "2.5.0",
        s"use {@code fromValue that return FromValue<?> instead",
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
        "return $LfromValue($L).fromValue($L)",
        classStaticAccessor,
        fromValueParams,
        "value$",
      )
      .build()
  }

  private def fromValue(
      constructor: String,
      typeParameters: IndexedSeq[String],
      className: TypeName,
      fieldType: Type,
      packagePrefixes: Map[PackageId, String],
  ) = {
    val converterParams =
      FromValueExtractorParameters.generate(typeParameters).fromValueParameterSpecs

    val fromValueCode = CodeBlock
      .builder()
      .add(FromValueGenerator.variantCheck(constructor, "value$", "variantValue$"))
      .addStatement(
        FromValueGenerator
          .generateFieldExtractor(
            fieldType,
            "body",
            CodeBlock.of("variantValue$$"),
            packagePrefixes,
          )
      )
      .addStatement("return new $T(body)", className)

    MethodSpec
      .methodBuilder("fromValue")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .addTypeVariables(className.typeParameters)
      .returns(ParameterizedTypeName.get(ClassName.get(classOf[FromValue[_]]), className))
      .addException(classOf[IllegalArgumentException])
      .addParameters(converterParams.asJava)
      .beginControlFlow("return $L ->", "value$")
      .addCode(fromValueCode.build())
      // put empty string in endControlFlow in order to have semicolon
      .endControlFlow("")
      .build()
  }

  private[inner] def lowerCaseFieldName(fieldName: String) =
    (fieldName.toList match {
      case Nil => List()
      case firstChar :: Nil => List(firstChar.toLower)
      case firstChar :: rest => firstChar.toLower +: rest
    }).mkString + "Value"
}
