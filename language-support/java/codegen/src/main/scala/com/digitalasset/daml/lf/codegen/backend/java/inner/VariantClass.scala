// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.TypeWithContext
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.typesig._
import PackageSignature.TypeDecl.Normal
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import javax.lang.model.element.Modifier

import scala.jdk.CollectionConverters._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
private[inner] object VariantClass extends StrictLogging {

  def generate(
      variantClassName: ClassName,
      subPackage: String,
      typeArguments: IndexedSeq[String],
      variant: Variant.FWT,
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String],
  ): (TypeSpec, List[TypeSpec]) =
    TrackLineage.of("variant", typeWithContext.name) {
      logger.info("Start")
      val constructorInfo = getFieldsWithTypes(variant.fields, packagePrefixes)
      val variantType = TypeSpec
        .classBuilder(variantClassName)
        .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
        .addTypeVariables(typeArguments.map(TypeVariableName.get).asJava)
        .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build())
        .addMethod(generateAbstractToValueSpec(typeArguments))
        .addMethod(generateFromValue(typeArguments, constructorInfo, variantClassName, subPackage))
        .addField(createPackageIdField(typeWithContext.interface.packageId))
        .build()
      val constructors = generateConstructorClasses(
        typeArguments,
        variant,
        typeWithContext,
        packagePrefixes,
        variantClassName,
      )
      logger.debug("End")
      (variantType, constructors)
    }

  private def isRecord(interfaceType: PackageSignature.TypeDecl): Boolean =
    interfaceType.`type`.dataType match {
      case _: Record[_] => true
      case _: Variant[_] | _: Enum => false
    }

  /** A record is a variant record if and only if
    * 1. it is part of the package where the variant is (i.e Package is None)
    * 2. its identifier has the same module as the variant
    * 3. its identifier name is equal to the variant identifier name with the constructor name appended
    */
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

  private def generateAbstractToValueSpec(typeArgs: IndexedSeq[String]): MethodSpec =
    MethodSpec
      .methodBuilder("toValue")
      .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
      .addParameters(ToValueExtractorParameters.generate(typeArgs).asJava)
      .returns(classOf[javaapi.data.Value])
      .build()

  private def initFromValueBuilder(t: TypeName): MethodSpec.Builder =
    MethodSpec
      .methodBuilder("fromValue")
      .addModifiers(Modifier.STATIC, Modifier.PUBLIC)
      .returns(t)
      .addParameter(classOf[javaapi.data.Value], "value$")

  private def variantExtractor(t: TypeName): CodeBlock =
    CodeBlock.of(
      "$T variant$$ = value$$.asVariant().orElseThrow(() -> new IllegalArgumentException($S))",
      classOf[javaapi.data.Variant],
      s"Expected Variant to build an instance of the Variant $t",
    )

  private def switchOnConstructor(
      builder: MethodSpec.Builder,
      constructors: Fields,
      variant: ClassName,
      subPackage: String,
      useConstructor: String => CodeBlock,
  ): MethodSpec.Builder = {
    val constructorsAsString = constructors.map(_.damlName).mkString("[", ", ", "]")
    logger.debug(s"Generating switch on constructors $constructorsAsString for $variant")
    for (constructorInfo <- constructors) {
      builder
        .beginControlFlow("if ($S.equals(variant$$.getConstructor()))", constructorInfo.damlName)
        .addStatement(useConstructor(List(subPackage, constructorInfo.javaName).mkString(".")))
        .endControlFlow()
    }
    builder
      .addStatement(
        "throw new IllegalArgumentException($S)",
        s"Found unknown constructor variant$$.getConstructor() for variant $variant, expected one of $constructorsAsString",
      )
  }

  private def generateParameterizedFromValue(
      variant: ParameterizedTypeName,
      constructors: Fields,
      subPackage: String,
  ): MethodSpec = {
    logger.debug(s"Generating fromValue static method for $variant")
    require(
      variant.typeArguments.asScala.forall(_.isInstanceOf[TypeVariableName]),
      s"All type arguments of ${variant.rawType} must be generic",
    )
    val builder = initFromValueBuilder(variant)
    val typeVariablesExtractorParameters =
      FromValueExtractorParameters.generate(
        variant.typeArguments.asScala.map(_.toString).toIndexedSeq
      )
    builder.addTypeVariables(typeVariablesExtractorParameters.typeVariables.asJava)
    builder.addParameters(typeVariablesExtractorParameters.functionParameterSpecs.asJava)
    builder.addStatement("$L", variantExtractor(variant.rawType))
    val extractors =
      CodeBlock.join(
        variant.typeArguments.asScala.map(t => CodeBlock.of("$L", s"fromValue$t")).asJava,
        ", ",
      )
    switchOnConstructor(
      builder,
      constructors,
      variant.rawType,
      subPackage,
      constructor => CodeBlock.of("return $L.fromValue(variant$$, $L)", constructor, extractors),
    )
    builder.build()
  }

  private def generateConcreteFromValue(
      t: ClassName,
      constructors: Fields,
      subPackage: String,
  ): MethodSpec = {
    logger.debug(s"Generating fromValue static method for $t")
    val builder = initFromValueBuilder(t).addStatement("$L", variantExtractor(t))
    switchOnConstructor(
      builder,
      constructors,
      t,
      subPackage,
      c => CodeBlock.of("return $L.fromValue(variant$$)", c),
    )
    builder.build()
  }

  private def generateFromValue(
      typeArguments: IndexedSeq[String],
      constructorInfo: Fields,
      variantClassName: ClassName,
      subPackage: String,
  ): MethodSpec =
    variantClassName.parameterized(typeArguments) match {
      case variant: ClassName => generateConcreteFromValue(variant, constructorInfo, subPackage)
      case variant: ParameterizedTypeName =>
        generateParameterizedFromValue(variant, constructorInfo, subPackage)
      case _ =>
        throw new IllegalArgumentException("Required either ClassName or ParameterizedTypeName")
    }

  private def generateConstructorClasses(
      typeArgs: IndexedSeq[String],
      variant: Variant.FWT,
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String],
      variantClassName: ClassName,
  ): List[TypeSpec] = {
    logger.debug("Generating inner classes")
    val innerClasses = new collection.mutable.ArrayBuffer[TypeSpec]
    val variantRecords = new collection.mutable.HashSet[String]()
    val fullVariantClassName = variantClassName.parameterized(typeArgs)
    for (fieldInfo <- getFieldsWithTypes(variant.fields, packagePrefixes)) {
      val FieldInfo(damlName, damlType, javaName, _) = fieldInfo
      damlType match {
        case TypeCon(TypeConName(id), _) if isVariantRecord(typeWithContext, damlName, id) =>
          // Variant records will be dealt with in a subsequent phase
          variantRecords.add(damlName)
        case _ =>
          logger.debug(s"$damlName is trivial")
          innerClasses += VariantConstructorClass.generate(
            typeWithContext.interface.packageId,
            fullVariantClassName,
            typeArgs,
            damlName,
            javaName,
            damlType,
            packagePrefixes,
          )
      }
    }
    for (child <- typeWithContext.typesLineages) yield {
      // A child of a variant can be either:
      // - a record of a constructor of the variant itself
      // - a type unrelated to the variant
      if (variantRecords.contains(child.name)) {
        logger.debug(s"${child.name} is a variant record")
        child.`type`.typ match {
          case Some(Normal(DefDataType(typeVars, record: Record.FWT))) =>
            innerClasses += VariantRecordClass
              .generate(
                typeWithContext.interface.packageId,
                typeVars.map(JavaEscaper.escapeString),
                getFieldsWithTypes(record.fields, packagePrefixes),
                child.name,
                fullVariantClassName,
                packagePrefixes,
              )
          case t =>
            val c = s"${typeWithContext.name}.${child.name}"
            throw new IllegalArgumentException(
              s"Underlying type of constructor $c is not Record (found: $t)"
            )
        }
      } else {
        logger.debug(s"${child.name} is an unrelated inner type")
      }
    }
    innerClasses.toList
  }

}
