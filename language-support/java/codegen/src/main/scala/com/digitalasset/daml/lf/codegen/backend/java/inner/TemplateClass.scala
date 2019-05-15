// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import java.util.Optional

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.{ContractId, CreatedEvent}
import com.digitalasset.daml.lf.codegen.TypeWithContext
import com.digitalasset.daml.lf.codegen.backend.java.ObjectMethods
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.digitalasset.daml.lf.iface._
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import javax.lang.model.element.Modifier

import scala.collection.JavaConverters._

private[inner] object TemplateClass extends StrictLogging {

  def generate(
      className: ClassName,
      record: Record.FWT,
      template: DefTemplate.FWT,
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String]): TypeSpec =
    TrackLineage.of("template", typeWithContext.name) {
      val fields = getFieldsWithTypes(record.fields, packagePrefixes)
      logger.info("Start")
      val staticCreateMethod = generateStaticCreateMethod(fields, className, packagePrefixes)

      val templateType = TypeSpec
        .classBuilder(className)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .superclass(classOf[javaapi.data.Template])
        .addField(generateTemplateIdField(typeWithContext))
        .addMethod(generateCreateMethod(className))
        .addMethods(
          generateCreateAndExerciseMethods(
            className,
            template.choices,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes))
        .addMethod(staticCreateMethod)
        .addType(
          generateIdClass(
            className,
            template.choices,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes))
        .addType(generateContractClass(className))
        .addFields(RecordFields(fields).asJava)
        .addMethods(RecordMethods(fields, className, IndexedSeq.empty, packagePrefixes).asJava)
        .build()
      logger.debug("End")
      templateType
    }

  private val idFieldName = "id"
  private val dataFieldName = "data"
  private val agreementFieldName = "agreementText"

  private val optionalString = ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])

  private def generateContractClass(templateClassName: ClassName): TypeSpec = {
    val contractIdClassName = ClassName.bestGuess("ContractId")
    val classBuilder =
      TypeSpec.classBuilder("Contract").addModifiers(Modifier.STATIC, Modifier.PUBLIC)
    classBuilder.addField(contractIdClassName, idFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(templateClassName, dataFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(optionalString, agreementFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addSuperinterface(ClassName.get(classOf[javaapi.data.Contract]))
    val constructorBuilder = MethodSpec
      .constructorBuilder()
      .addModifiers(Modifier.PUBLIC)
      .addParameter(contractIdClassName, idFieldName)
      .addParameter(templateClassName, dataFieldName)
      .addParameter(optionalString, agreementFieldName)
    constructorBuilder.addStatement("this.$L = $L", idFieldName, idFieldName)
    constructorBuilder.addStatement("this.$L = $L", dataFieldName, dataFieldName)
    constructorBuilder.addStatement("this.$L = $L", agreementFieldName, agreementFieldName)
    val constructor = constructorBuilder.build()

    classBuilder.addMethod(constructor)

    val contractClassName = ClassName.bestGuess("Contract")
    val fields = Array(idFieldName, dataFieldName, agreementFieldName)
    classBuilder
      .addMethod(generateFromIdAndRecord(contractClassName, templateClassName, contractIdClassName))
      .addMethod(
        generateFromIdAndRecordDeprecated(
          contractClassName,
          templateClassName,
          contractIdClassName))
      .addMethod(
        generateFromCreatedEvent(contractClassName, templateClassName, contractIdClassName))
      .addMethods(ObjectMethods(contractClassName, fields, templateClassName).asJava)
      .build()
  }

  private[inner] def generateFromIdAndRecord(
      className: ClassName,
      templateClassName: ClassName,
      idClassName: ClassName): MethodSpec =
    MethodSpec
      .methodBuilder("fromIdAndRecord")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(className)
      .addParameter(classOf[String], "contractId")
      .addParameter(classOf[javaapi.data.Record], "record$")
      .addParameter(
        ParameterizedTypeName.get(classOf[Optional[_]], classOf[String]),
        agreementFieldName)
      .addStatement("$T $L = new $T(contractId)", idClassName, idFieldName, idClassName)
      .addStatement(
        "$T $L = $T.fromValue(record$$)",
        templateClassName,
        dataFieldName,
        templateClassName)
      .addStatement(
        "return new $T($L, $L, $L)",
        className,
        idFieldName,
        dataFieldName,
        agreementFieldName)
      .build()

  private[inner] def generateFromIdAndRecordDeprecated(
      className: ClassName,
      templateClassName: ClassName,
      idClassName: ClassName): MethodSpec =
    MethodSpec
      .methodBuilder("fromIdAndRecord")
      .addAnnotation(classOf[Deprecated])
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(className)
      .addParameter(classOf[String], "contractId")
      .addParameter(classOf[javaapi.data.Record], "record$")
      .addStatement("$T $L = new $T(contractId)", idClassName, idFieldName, idClassName)
      .addStatement(
        "$T $L = $T.fromValue(record$$)",
        templateClassName,
        dataFieldName,
        templateClassName)
      .addStatement(
        "return new $T($L, $L, $T.empty())",
        className,
        idFieldName,
        dataFieldName,
        classOf[Optional[_]])
      .build()

  private[inner] def generateFromCreatedEvent(
      className: ClassName,
      templateClassName: ClassName,
      idClassName: ClassName) = {
    MethodSpec
      .methodBuilder("fromCreatedEvent")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(className)
      .addParameter(classOf[CreatedEvent], "event")
      .addStatement(
        "return fromIdAndRecord(event.getContractId(), event.getArguments(), event.getAgreementText())")
      .build()
  }

  private def generateCreateMethod(name: ClassName): MethodSpec =
    MethodSpec
      .methodBuilder("create")
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.CreateCommand])
      .addStatement(
        "return new $T($T.TEMPLATE_ID, this.toValue())",
        classOf[javaapi.data.CreateCommand],
        name)
      .build()

  private def generateStaticCreateMethod(
      fields: Fields,
      name: ClassName,
      packagePrefixes: Map[PackageId, String]): MethodSpec =
    fields
      .foldLeft(
        MethodSpec
          .methodBuilder("create")
          .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
          .returns(classOf[javaapi.data.CreateCommand])) {
        case (b, FieldInfo(_, _, escapedName, tpe)) => b.addParameter(tpe, escapedName)
      }
      .addStatement(
        "return new $T($L).create()",
        name,
        generateArgumentList(fields.map(_.javaName)))
      .build()

  private def generateIdClass(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[com.digitalasset.daml.lf.iface.Type]],
      typeDeclarations: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
      packagePrefixes: Map[PackageId, String]): TypeSpec = {

    val idClassBuilder =
      TypeSpec
        .classBuilder("ContractId")
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC)
        .addField(ClassName.get(classOf[String]), "contractId", Modifier.PUBLIC, Modifier.FINAL)
    val constructor =
      MethodSpec
        .constructorBuilder()
        .addModifiers(Modifier.PUBLIC)
        .addParameter(ClassName.get(classOf[String]), "contractId")
        .addStatement("this.contractId = contractId")
        .build()
    idClassBuilder.addMethod(constructor)
    for ((choiceName, choice) <- choices) {
      val exerciseChoiceMethod =
        generateExerciseMethod(choiceName, choice, templateClassName, packagePrefixes)
      idClassBuilder.addMethod(exerciseChoiceMethod)
      for (record <- choice.param.fold(
          getRecord(_, typeDeclarations, packageId),
          _ => None,
          _ => None)) {
        val splatted = generateFlattenedExerciseMethod(
          choiceName,
          choice,
          getFieldsWithTypes(record.fields, packagePrefixes),
          packagePrefixes)
        idClassBuilder.addMethod(splatted)
      }
    }
    val toValue = MethodSpec
      .methodBuilder("toValue")
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.Value])
      .addStatement(
        CodeBlock.of("return new $L(this.contractId)", ClassName.get(classOf[ContractId])))
      .build()
    idClassBuilder.addMethod(toValue)

    idClassBuilder.addMethods(ObjectMethods(
      ClassName.bestGuess("ContractId"),
      IndexedSeq("contractId"),
      templateClassName).asJava)
    idClassBuilder.build()
  }

  private def getRecord(
      typeCon: TypeCon,
      identifierToType: Map[QualifiedName, InterfaceType],
      packageId: PackageId
  ): Option[Record.FWT] = {
    // TODO: at the moment we don't support other packages Records because the codegen works on single packages
    if (typeCon.name.identifier.packageId == packageId) {
      identifierToType.get(typeCon.name.identifier.qualifiedName) match {
        case Some(InterfaceType.Normal(DefDataType(_, record: Record.FWT))) =>
          Some(record)
        case _ => None
      }
    } else None
  }

  private def generateCreateAndExerciseMethods(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[com.digitalasset.daml.lf.iface.Type]],
      typeDeclarations: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
      packagePrefixes: Map[PackageId, String]) = {
    val methods = for ((choiceName, choice) <- choices) yield {
      val createAndExerciseChoiceMethod =
        generateCreateAndExerciseMethod(choiceName, choice, templateClassName, packagePrefixes)
      val splatted = for (record <- choice.param
          .fold(getRecord(_, typeDeclarations, packageId), _ => None, _ => None)) yield {
        generateFlattenedCreateAndExerciseMethod(
          choiceName,
          choice,
          getFieldsWithTypes(record.fields, packagePrefixes),
          packagePrefixes)
      }
      createAndExerciseChoiceMethod :: splatted.toList
    }
    methods.flatten.asJava

  }

  private def generateCreateAndExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      templateClassName: ClassName,
      packagePrefixes: Map[PackageId, String]): MethodSpec = {
    val methodName = s"createAndExercise${choiceName.capitalize}"
    val createAndExerciseChoiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.CreateAndExerciseCommand])
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    createAndExerciseChoiceBuilder.addParameter(javaType, "arg")
    val choiceArgument = choice.param match {
      case TypeCon(_, _) => "arg.toValue()"
      case TypePrim(_, _) => "arg"
      case TypeVar(_) => "arg"
    }
    createAndExerciseChoiceBuilder.addStatement(
      "return new $T($T.TEMPLATE_ID, this.toValue(), $S, $L)",
      classOf[javaapi.data.CreateAndExerciseCommand],
      templateClassName,
      choiceName,
      choiceArgument)
    createAndExerciseChoiceBuilder.build()
  }

  private def generateFlattenedCreateAndExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      fields: Fields,
      packagePrefixes: Map[PackageId, String]): MethodSpec = {
    val methodName = s"createAndExercise${choiceName.capitalize}"
    val createAndExerciseChoiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.CreateAndExerciseCommand])
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    for (FieldInfo(_, _, javaName, javaType) <- fields) {
      createAndExerciseChoiceBuilder.addParameter(javaType, javaName)
    }
    createAndExerciseChoiceBuilder.addStatement(
      "return $L(new $T($L))",
      methodName,
      javaType,
      generateArgumentList(fields.map(_.javaName)))
    createAndExerciseChoiceBuilder.build()
  }

  private def generateExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      templateClassName: ClassName,
      packagePrefixes: Map[PackageId, String]): MethodSpec = {
    val methodName = s"exercise${choiceName.capitalize}"
    val exerciseChoiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.ExerciseCommand])
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    exerciseChoiceBuilder.addParameter(javaType, "arg")
    val choiceArgument = choice.param match {
      case TypeCon(_, _) => "arg.toValue()"
      case TypePrim(_, _) => "arg"
      case TypeVar(_) => "arg"
    }
    exerciseChoiceBuilder.addStatement(
      "return new $T($T.TEMPLATE_ID, this.contractId, $S, $L)",
      classOf[javaapi.data.ExerciseCommand],
      templateClassName,
      choiceName,
      choiceArgument)
    exerciseChoiceBuilder.build()
  }

  private def generateFlattenedExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      fields: Fields,
      packagePrefixes: Map[PackageId, String]): MethodSpec = {
    val methodName = s"exercise${choiceName.capitalize}"
    val exerciseChoiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.ExerciseCommand])
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    for (FieldInfo(_, _, javaName, javaType) <- fields) {
      exerciseChoiceBuilder.addParameter(javaType, javaName)
    }
    exerciseChoiceBuilder.addStatement(
      "return $L(new $T($L))",
      methodName,
      javaType,
      generateArgumentList(fields.map(_.javaName)))
    exerciseChoiceBuilder.build()
  }

  private def generateTemplateIdField(typeWithContext: TypeWithContext): FieldSpec =
    FieldSpec
      .builder(
        ClassName.get(classOf[javaapi.data.Identifier]),
        "TEMPLATE_ID",
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC)
      .initializer(
        "new $T($S, $S, $S)",
        classOf[javaapi.data.Identifier],
        typeWithContext.packageId,
        typeWithContext.modulesLineage.map(_._1).toImmArray.iterator.mkString("."),
        typeWithContext.name
      )
      .build()

}
