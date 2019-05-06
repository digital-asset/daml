// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.ContractId
import com.digitalasset.daml.lf.codegen.TypeWithContext
import com.digitalasset.daml.lf.codegen.backend.java.ObjectMethods
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, QualifiedName, PackageId}
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

  private def generateContractClass(templateClassName: ClassName): TypeSpec = {
    val contractIdClassName = ClassName.bestGuess("ContractId")
    val idFieldName = "id"
    val dataFieldName = "data"
    val classBuilder =
      TypeSpec.classBuilder("Contract").addModifiers(Modifier.STATIC, Modifier.PUBLIC)
    classBuilder.addField(contractIdClassName, idFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(templateClassName, dataFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addSuperinterface(ClassName.get(classOf[javaapi.data.Contract]))
    val constructorBuilder = MethodSpec
      .constructorBuilder()
      .addModifiers(Modifier.PUBLIC)
      .addParameter(contractIdClassName, idFieldName)
      .addParameter(templateClassName, dataFieldName)
    constructorBuilder.addStatement("this.$L = $L", idFieldName, idFieldName)
    constructorBuilder.addStatement("this.$L = $L", dataFieldName, dataFieldName)
    val constructor = constructorBuilder.build()
    classBuilder.addMethod(constructor)
    val contractClassName = ClassName.bestGuess("Contract")
    val fields = Array(idFieldName -> contractIdClassName, dataFieldName -> templateClassName)
    classBuilder.addMethod(
      generateFromIdAndRecord(contractClassName, templateClassName, contractIdClassName))
    classBuilder.addMethods(
      ObjectMethods(contractClassName, fields.map(_._1), templateClassName).asJava)
    classBuilder.build()
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
      .addStatement("$T id = new $T(contractId)", idClassName, idClassName)
      .addStatement("$T data = $T.fromValue(record$$)", templateClassName, templateClassName)
      .addStatement("return new $T(id, data)", className)
      .build()

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
    if (typeCon.name.ref.packageId == packageId) {
      identifierToType.get(typeCon.name.ref.qualifiedName) match {
        case Some(InterfaceType.Normal(DefDataType(_, record: Record.FWT))) =>
          Some(record)
        case _ => None
      }
    } else None
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
