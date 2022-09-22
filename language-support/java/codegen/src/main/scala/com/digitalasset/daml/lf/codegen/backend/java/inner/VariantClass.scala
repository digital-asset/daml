// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.TypeWithContext
import com.daml.lf.codegen.backend.java.JavaEscaper
import com.daml.lf.data.Ref.{Identifier, PackageId}
import com.daml.lf.typesig._
import PackageSignature.TypeDecl.Normal
import com.daml.ledger.javaapi.data.codegen.FromValue
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
        .addMethod(
          generateDeprecatedFromValue(typeArguments, variantClassName)
        )
        .addMethod(generateFromValue(typeArguments, constructorInfo, variantClassName))
        .addMethods(
          VariantValueDecodersMethods(
            typeArguments,
            variant,
            typeWithContext,
            packagePrefixes,
            subPackage,
          ).asJava
        )
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

  private def variantExtractor(t: TypeName): CodeBlock =
    CodeBlock.of(
      "$T variant$$ = value$$.asVariant().orElseThrow(() -> new IllegalArgumentException($S))",
      classOf[javaapi.data.Variant],
      s"Expected Variant to build an instance of the Variant $t",
    )

  private def switchOnConstructor(
      builder: CodeBlock.Builder,
      constructors: Fields,
      variant: ClassName,
      useValueDecoder: String => CodeBlock,
  ): CodeBlock.Builder = {
    val constructorsAsString = constructors.map(_.damlName).mkString("[", ", ", "]")
    logger.debug(s"Generating switch on constructors $constructorsAsString for $variant")
    for (constructorInfo <- constructors) {
      builder
        .beginControlFlow("if ($S.equals(variant$$.getConstructor()))", constructorInfo.damlName)
        .addStatement(useValueDecoder(s"fromValue${constructorInfo.javaName}"))
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
  ): MethodSpec = {
    logger.debug(s"Generating fromValue static method for $variant")
    require(
      variant.typeArguments.asScala.forall(_.isInstanceOf[TypeVariableName]),
      s"All type arguments of ${variant.rawType} must be generic",
    )
    val returnType = ParameterizedTypeName.get(ClassName.get(classOf[FromValue[_]]), variant)
    val builder = initFromValueBuilder(returnType)
    builder.beginControlFlow("return $L ->", "value$")

    val typeVariablesExtractorParameters =
      FromValueExtractorParameters.generate(
        variant.typeArguments.asScala.map(_.toString).toIndexedSeq
      )

    builder.addTypeVariables(typeVariablesExtractorParameters.typeVariables.asJava)
    builder.addParameters(typeVariablesExtractorParameters.fromValueParameterSpecs.asJava)

    val decodeValueCodeBuilder = CodeBlock
      .builder()

    decodeValueCodeBuilder.addStatement("$L", variantExtractor(variant.rawType))
    val extractors =
      CodeBlock.join(
        variant.typeArguments.asScala.map(t => CodeBlock.of("$L", s"fromValue$t")).asJava,
        ", ",
      )
    switchOnConstructor(
      decodeValueCodeBuilder,
      constructors,
      variant.rawType,
      valueDecoder => CodeBlock.of("return $L($L).fromValue(variant$$)", valueDecoder, extractors),
    )

    builder
      .addCode(decodeValueCodeBuilder.build())
      .endControlFlow("")
      .build()
  }

  private def generateConcreteFromValue(
      t: ClassName,
      constructors: Fields,
  ): MethodSpec = {
    logger.debug(s"Generating fromValue static method for $t")
    val returnType = ParameterizedTypeName.get(ClassName.get(classOf[FromValue[_]]), t)
    val builder = initFromValueBuilder(returnType)
      .beginControlFlow("return $L ->", "value$")

    val decodeValueCodeBuilder = CodeBlock
      .builder()
      .addStatement("$L", variantExtractor(t))

    switchOnConstructor(
      decodeValueCodeBuilder,
      constructors,
      t,
      valueDecoder => CodeBlock.of("return $L().fromValue(variant$$)", valueDecoder),
    )

    builder
      .addCode(decodeValueCodeBuilder.build())
      .endControlFlow("")
      .build()
  }

  private def generateDeprecatedFromValue(
      typeArguments: IndexedSeq[String],
      variantClassName: ClassName,
  ): MethodSpec =
    variantClassName.parameterized(typeArguments) match {
      case variant: ClassName =>
        generateDeprecatedConcreteFromValue(variant)
      case variant: ParameterizedTypeName =>
        generateDeprecatedParameterizedFromValue(variant)
      case _ =>
        throw new IllegalArgumentException("Required either ClassName or ParameterizedTypeName")
    }

  private def generateDeprecatedConcreteFromValue(
      t: ClassName
  ): MethodSpec = {
    logger.debug(s"Generating depreacted fromValue static method for $t")
    val builder = initFromValueBuilder(t)
      .addParameter(classOf[javaapi.data.Value], "value$")
      .addAnnotation(classOf[Deprecated])
      .addJavadoc(
        "@deprecated since Daml $L; $L",
        "2.5.0",
        s"use {@code fromValue} that return FromValue<?> instead",
      )
      .addStatement("$L", variantExtractor(t))
    builder.addStatement(
      "return fromValue().fromValue($L)",
      "value$",
    )
    builder.build()
  }

  private def generateDeprecatedParameterizedFromValue(
      variant: ParameterizedTypeName
  ): MethodSpec = {
    logger.debug(s"Generating deprecated fromValue static method for $variant")
    require(
      variant.typeArguments.asScala.forall(_.isInstanceOf[TypeVariableName]),
      s"All type arguments of ${variant.rawType} must be generic",
    )
    val builder = initFromValueBuilder(variant)
      .addParameter(classOf[javaapi.data.Value], "value$")

    val typeVariablesExtractorParameters =
      FromValueExtractorParameters.generate(
        variant.typeArguments.asScala.map(_.toString).toIndexedSeq
      )
    builder
      .addTypeVariables(typeVariablesExtractorParameters.typeVariables.asJava)
      .addParameters(typeVariablesExtractorParameters.functionParameterSpecs.asJava)
      .addAnnotation(classOf[Deprecated])
      .addJavadoc(
        "@deprecated since Daml $L; $L",
        "2.5.0",
        s"use {@code fromValue} that return FromValue<?> instead",
      )
    val fromValueParams = CodeBlock.join(
      typeVariablesExtractorParameters.functionParameterSpecs.map { param =>
        CodeBlock.of("$N::apply", param)
      }.asJava,
      ", ",
    )

    val classStaticAccessor = {
      val typeParameterList = CodeBlock.join(
        variant.typeArguments.asScala.map { param =>
          CodeBlock.of("$T", param)
        }.asJava,
        ", ",
      )
      CodeBlock.of("$T.<$L>", variant.rawType, typeParameterList)
    }

    builder.addStatement(
      "return $LfromValue($L).fromValue($L)",
      classStaticAccessor,
      fromValueParams,
      "value$",
    )
    builder.build()
  }

  private def generateFromValue(
      typeArguments: IndexedSeq[String],
      constructorInfo: Fields,
      variantClassName: ClassName,
  ): MethodSpec =
    variantClassName.parameterized(typeArguments) match {
      case variant: ClassName => generateConcreteFromValue(variant, constructorInfo)
      case variant: ParameterizedTypeName =>
        generateParameterizedFromValue(variant, constructorInfo)
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
