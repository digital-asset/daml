// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.lf.typesig.{Type, Variant}
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.codegen.TypeWithContext
import com.daml.lf.typesig._
import com.squareup.javapoet._
import PackageSignature.TypeDecl.Normal
import com.daml.ledger.javaapi.data.codegen.ValueDecoder

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

object VariantValueDecodersMethods {
  def apply(
      typeArgs: IndexedSeq[String],
      variant: Variant.FWT,
      typeWithContext: TypeWithContext,
      subPackage: String,
  )(implicit packagePrefixes: PackagePrefixes): Vector[MethodSpec] = {
    val (variantRecords, methodSpecs) =
      getFieldsWithTypes(variant.fields).partitionMap { fieldInfo =>
        val FieldInfo(damlName, damlType, javaName, _) = fieldInfo
        damlType match {
          case TypeCon(TypeConName(id), _) if isVariantRecord(typeWithContext, damlName, id) =>
            // Variant records will be dealt with in a subsequent phase
            Left(damlName)
          case _ =>
            val className =
              ClassName.bestGuess(s"$subPackage.$javaName").parameterized(typeArgs)
            Right(
              variantConDecoderMethod(damlName, typeArgs, className, damlType)
            )
        }
      }

    val recordAddons = for {
      child <- typeWithContext.typesLineages
      if variantRecords.contains(child.name)
    } yield {
      // A child of a variant can be either:
      // - a record of a constructor of the variant itself
      // - a type unrelated to the variant
      child.`type`.typ match {
        case Some(Normal(DefDataType(typeVars, record: Record.FWT))) =>
          val typeParameters = typeVars.map(JavaEscaper.escapeString)
          val className =
            ClassName.bestGuess(s"$subPackage.${child.name}").parameterized(typeParameters)

          FromValueGenerator.generateValueDecoderForRecordLike(
            getFieldsWithTypes(record.fields),
            className,
            typeArgs,
            s"valueDecoder${child.name}",
            FromValueGenerator.variantCheck(child.name, _, _),
          )
        case t =>
          val c = s"${typeWithContext.name}.${child.name}"
          throw new IllegalArgumentException(
            s"Underlying type of constructor $c is not Record (found: $t)"
          )
      }
    }
    (methodSpecs ++ recordAddons).toVector
  }

  private def variantConDecoderMethod(
      constructor: String,
      typeParameters: IndexedSeq[String],
      className: TypeName,
      fieldType: Type,
  )(implicit
      packagePrefixes: PackagePrefixes
  ) = {
    val converterParams =
      FromValueExtractorParameters.generate(typeParameters).valueDecoderParameterSpecs

    val valueDecoderCode = CodeBlock
      .builder()
      .add(FromValueGenerator.variantCheck(constructor, "value$", "variantValue$"))
      .addStatement(
        FromValueGenerator
          .generateFieldExtractor(
            fieldType,
            "body",
            CodeBlock.of("variantValue$$"),
          )
      )
      .addStatement("return new $T(body)", className)

    MethodSpec
      .methodBuilder(s"valueDecoder$constructor")
      .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
      .addTypeVariables(className.typeParameters)
      .returns(ParameterizedTypeName.get(ClassName.get(classOf[ValueDecoder[_]]), className))
      .addException(classOf[IllegalArgumentException])
      .addParameters(converterParams.asJava)
      .beginControlFlow("return $L ->", "value$")
      .addCode(valueDecoderCode.build())
      // put empty string in endControlFlow in order to have semicolon
      .endControlFlow("")
      .build()
  }
}
