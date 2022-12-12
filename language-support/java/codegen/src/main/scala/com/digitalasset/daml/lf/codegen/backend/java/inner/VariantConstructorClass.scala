// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data
import com.daml.ledger.javaapi.data.Value
import com.daml.ledger.javaapi.data.codegen.{PrimitiveValueDecoders, ValueDecoder}
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
  )(implicit
      packagePrefixes: PackagePrefixes
  ): TypeSpec = {
    TrackLineage.of("variant constructor", constructorName) {
      logger.info("Start")

      val className = ClassName.bestGuess(javaName).parameterized(typeArgs)
      val javaType = toJavaTypeName(body)
      val variantFieldName = lowerCaseFieldName(body match {
        case TypeVar(typeArg) =>
          JavaEscaper.escapeString(typeArg)
        case _ =>
          javaType.rawType.simpleName
      })

      val constructor = ConstructorGenerator.generateConstructor(
        IndexedSeq(FieldInfo("body", body, variantFieldName, javaType))
      )

      val conversionMethods = distinctTypeVars(body, typeArgs) match {
        case IndexedSeq(params) =>
          List(
            toValue(constructorName, params, body, variantFieldName),
            deprecatedFromValue(params, params, variant, className),
          )
        case IndexedSeq(usedParams, allParams) =>
          // usedParams is always subset of allParams
          List(
            toValue(constructorName, usedParams, body, variantFieldName),
            deprecatedFromValue(usedParams, allParams, variant, className),
            toValue(constructorName, allParams, body, variantFieldName),
            deprecatedFromValue(allParams, allParams, variant, className),
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
  )(implicit
      packagePrefixes: PackagePrefixes
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
          ),
      )
      .build()
  }

  // TODO #15120 delete
  private def deprecatedFromValue(
      typeParameters: IndexedSeq[String],
      allTypeParameters: IndexedSeq[String],
      variantClass: TypeName,
      className: TypeName,
  ) = {
    val valueParam = ParameterSpec.builder(classOf[Value], "value$").build()

    val extractorParams =
      FromValueExtractorParameters.generate(typeParameters)

    val converterParams =
      extractorParams.functionParameterSpecs

    val converterParamsNameSet = converterParams.map(_.name).toSet

    val allConverterParams =
      FromValueExtractorParameters.generate(allTypeParameters).functionParameterSpecs

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
        s"use {@code valueDecoder} instead",
      )

    val typeParamsValueDecoders = CodeBlock.join(
      allConverterParams.map { param =>
        if (converterParamsNameSet.contains(param.name))
          CodeBlock.of("$T.fromFunction($N)", classOf[ValueDecoder[_]], param)
        else
          CodeBlock.of("$T.impossible()", classOf[PrimitiveValueDecoders])
      }.asJava,
      ",$W",
    )

    val classStaticAccessor = if (className.typeParameters.size > 0) {
      val typeParameterList = CodeBlock.join(
        className.typeParameters.asScala.map { param =>
          CodeBlock.of("$T", param)
        }.asJava,
        ",$W",
      )
      CodeBlock.of("$T.<$L>", variantClass.rawType, typeParameterList)
    } else CodeBlock.of("")

    method
      .addStatement(
        "return ($T)$LvalueDecoder($L).decode($L)",
        className,
        classStaticAccessor,
        typeParamsValueDecoders,
        "value$",
      )
      .build()
  }

  private[inner] def lowerCaseFieldName(fieldName: String) =
    (fieldName.toList match {
      case Nil => List()
      case firstChar :: Nil => List(firstChar.toLower)
      case firstChar :: rest => firstChar.toLower +: rest
    }).mkString + "Value"
}
