// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.lf.typesig.{Type, Variant}
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.ledger.javaapi.data.codegen.FromValue
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.codegen.TypeWithContext
import com.daml.lf.typesig._
import com.squareup.javapoet._
import PackageSignature.TypeDecl.Normal

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

object VariantValueDecodersMethods {
  def apply(
      typeArgs: IndexedSeq[String],
      variant: Variant.FWT,
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String],
      subPackage: String,
  ): Vector[MethodSpec] = {
    val methodSpecs = new collection.mutable.ArrayBuffer[MethodSpec]
    val variantRecords = new collection.mutable.HashSet[String]()

    for (fieldInfo <- getFieldsWithTypes(variant.fields, packagePrefixes)) {
      val FieldInfo(damlName, damlType, javaName, _) = fieldInfo
      damlType match {
        case TypeCon(TypeConName(id), _) if isVariantRecord(typeWithContext, damlName, id) =>
          // Variant records will be dealt with in a subsequent phase
          variantRecords.add(damlName)
        case _ =>
          val className =
            ClassName.bestGuess(s"$subPackage.$javaName").parameterized(typeArgs)
          println("!! >> variant con " + className)
          methodSpecs +=
            variantConDecoderMethod(damlName, typeArgs, className, damlType, packagePrefixes)
      }
    }
    for (child <- typeWithContext.typesLineages) yield {
      // A child of a variant can be either:
      // - a record of a constructor of the variant itself
      // - a type unrelated to the variant
      if (variantRecords.contains(child.name)) {
        child.`type`.typ match {
          case Some(Normal(DefDataType(typeVars, record: Record.FWT))) =>
            val typeParameters = typeVars.map(JavaEscaper.escapeString)
            val className =
              ClassName.bestGuess(s"$subPackage.${child.name}").parameterized(typeParameters)

            methodSpecs += FromValueGenerator.generateFromValueForRecordLike(
              getFieldsWithTypes(record.fields, packagePrefixes),
              className,
              typeArgs,
              s"fromValue${child.name}",
              FromValueGenerator.variantCheck(child.name, _, _),
              packagePrefixes,
            )
          case t =>
            val c = s"${typeWithContext.name}.${child.name}"
            throw new IllegalArgumentException(
              s"Underlying type of constructor $c is not Record (found: $t)"
            )
        }
      }
    }
    methodSpecs.toVector
  }

  private def variantConDecoderMethod(
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
      .methodBuilder(s"fromValue$constructor")
      .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
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

  // TODO: CL extract
  private def isVariantRecord(
      typeWithContext: TypeWithContext,
      constructor: String,
      identifier: Identifier,
  ): Boolean = {
    typeWithContext.interface.typeDecls.get(identifier.qualifiedName).exists(isRecord) &&
    typeWithContext.identifier.qualifiedName.module == identifier.qualifiedName.module &&
    typeWithContext.identifier.qualifiedName.name.segments == identifier.qualifiedName.name.segments.init &&
    constructor == identifier.qualifiedName.name.segments.last
  }

  // TODO: CL extract
  private def isRecord(interfaceType: PackageSignature.TypeDecl): Boolean =
    interfaceType.`type`.dataType match {
      case _: Record[_] => true
      case _: Variant[_] | _: Enum => false
    }
}
