// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data
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
  ): (TypeSpec, Seq[(ClassName, String)]) = {
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
            toValue(constructorName, params, body, variantFieldName)
          )
        case IndexedSeq(usedParams, allParams) =>
          // usedParams is always subset of allParams
          List(
            toValue(constructorName, usedParams, body, variantFieldName),
            toValue(constructorName, allParams, body, variantFieldName),
          )
      }

      val (jsonEncoderMethods, staticImports) =
        ToJsonGenerator.forVariantSimple(javaName, typeArgs, variantFieldName, body)

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
          .addMethods(jsonEncoderMethods.asJava)
          .build()

      logger.info("End")
      (typeSpec, staticImports)
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

  private[inner] def lowerCaseFieldName(fieldName: String) =
    (fieldName.toList match {
      case Nil => List()
      case firstChar :: Nil => List(firstChar.toLower)
      case firstChar :: rest => firstChar.toLower +: rest
    }).mkString + "Value"
}
