// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{PrimitiveValueDecoders, ValueDecoder}
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

private[inner] object VariantRecordMethods extends StrictLogging {

  def apply(
      constructorName: String,
      fields: Fields,
      variantClassName: TypeName,
      className: TypeName,
      typeParameters: IndexedSeq[String],
  )(implicit
      packagePrefixes: PackagePrefixes
  ): Vector[MethodSpec] = {
    val constructor = ConstructorGenerator.generateConstructor(fields)

    val conversionMethods = distinctTypeVars(fields, typeParameters) match {
      case IndexedSeq(params) =>
        List(
          toValue(constructorName, params, fields),
          generateDeprecatedFromValue(params, params, variantClassName, className),
        )
      case IndexedSeq(usedParams, allParams) =>
        // usedParams is always subset of allParams
        List(
          toValue(constructorName, usedParams, fields),
          generateDeprecatedFromValue(usedParams, allParams, variantClassName, className),
          toValue(constructorName, allParams, fields),
          generateDeprecatedFromValue(allParams, allParams, variantClassName, className),
        )
    }

    Vector(constructor) ++ conversionMethods ++
      ObjectMethods(className.rawType, typeParameters, fields.map(_.javaName))
  }

  private def toValue(constructorName: String, params: IndexedSeq[String], fields: Fields)(implicit
      packagePrefixes: PackagePrefixes
  ) = ToValueGenerator.generateToValueForRecordLike(
    params,
    fields,
    TypeName.get(classOf[javaapi.data.Variant]),
    name =>
      CodeBlock.of(
        "return new $T($S, new $T($L))",
        classOf[javaapi.data.Variant],
        constructorName,
        classOf[javaapi.data.DamlRecord],
        name,
      ),
  )

  // TODO #15120 delete
  private def generateDeprecatedFromValue(
      typeParameters: IndexedSeq[String],
      allTypeParameters: IndexedSeq[String],
      variantClassName: TypeName,
      className: TypeName,
  ): MethodSpec = {
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
      .returns(className)
      .addTypeVariables(className.typeParameters)
      .addParameter(TypeName.get(classOf[javaapi.data.Value]), "value$")
      .addParameters(converterParams.asJava)
      .addException(classOf[IllegalArgumentException])
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
      CodeBlock.of("$T.<$L>", variantClassName.rawType, typeParameterList)
    } else CodeBlock.of("")

    method
      .addStatement(
        "return ($T)$LvalueDecoder($L).decode($L)",
        className,
        classStaticAccessor,
        typeParamsValueDecoders,
        "value$",
      )
      .build
  }

}
