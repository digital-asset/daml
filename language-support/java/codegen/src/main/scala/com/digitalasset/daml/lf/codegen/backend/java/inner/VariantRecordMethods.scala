// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.daml.lf.data.Ref.PackageId
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

private[inner] object VariantRecordMethods extends StrictLogging {

  private def toValue(
      constructorName: String,
      params: IndexedSeq[String],
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
  ) = ToValueGenerator.generateToValueForRecordLike(
    params,
    fields,
    packagePrefixes,
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

  def apply(
      constructorName: String,
      fields: Fields,
      variantClassName: TypeName,
      className: TypeName,
      typeParameters: IndexedSeq[String],
      packagePrefixes: Map[PackageId, String],
  ): Vector[MethodSpec] = {
    val constructor = ConstructorGenerator.generateConstructor(fields)

    val conversionMethods = distinctTypeVars(fields, typeParameters) match {
      case IndexedSeq(params) =>
        List(
          toValue(constructorName, params, fields, packagePrefixes),
          generateDeprecatedFromValue(params, params, variantClassName, className),
        )
      case IndexedSeq(usedParams, allParams) =>
        // usedParams is always subset of allParams
        List(
          toValue(constructorName, usedParams, fields, packagePrefixes),
          generateDeprecatedFromValue(usedParams, allParams, variantClassName, className),
          toValue(constructorName, allParams, fields, packagePrefixes),
          generateDeprecatedFromValue(allParams, allParams, variantClassName, className),
        )
    }

    Vector(constructor) ++ conversionMethods ++
      ObjectMethods(className.rawType, typeParameters, fields.map(_.javaName))
  }

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
        s"use {@code fromValue} that return FromValue<?> instead",
      )

    val typeParamsValueDecoders = CodeBlock.join(
      allConverterParams.map { param =>
        if (converterParamsNameSet.contains(param.name))
          CodeBlock.of("$N::apply", param)
        else
          CodeBlock.of("$T.impossible()", classOf[PrimitiveValueDecoders])
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
      CodeBlock.of("$T.<$L>", variantClassName.rawType, typeParameterList)
    } else CodeBlock.of("")

    method
      .addStatement(
        "return ($T)$LfromValue($L).fromValue($L)",
        className,
        classStaticAccessor,
        typeParamsValueDecoders,
        "value$",
      )
      .build
  }

}
