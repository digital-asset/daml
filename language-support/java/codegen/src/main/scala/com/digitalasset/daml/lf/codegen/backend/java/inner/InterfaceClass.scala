// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.backend.java.{JavaEscaper, ObjectMethods}
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.iface.{
  DefInterface,
  PrimType,
  TemplateChoice,
  Type,
  TypeCon,
  TypeNumeric,
  TypePrim,
  TypeVar,
}
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging

import java.util.Optional
import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

object InterfaceClass extends StrictLogging {

  def classNameForInterface(qualifiedName: QualifiedName) =
    ClassName.bestGuess {
      val QualifiedName(module, name) = qualifiedName
      // consider all but the last name segment to be part of the java package name
      val packageSegments = module.segments.slowAppend(name.segments).toSeq.dropRight(1)
      // consider the last name segment to be the java class name
      val className = name.segments.toSeq.takeRight(1)

      val packageName = packageSegments.map(_.toLowerCase)

      (packageName ++ className)
        .filter(_.nonEmpty)
        .map(JavaEscaper.escapeString)
        .mkString(".")
    }

  def generate(
      interfaceName: ClassName,
      interface: DefInterface.FWT,
      packagePrefixes: Map[PackageId, String],
      packageId: PackageId,
      interfaceId: QualifiedName,
  ): TypeSpec =
    TrackLineage.of("interface", interfaceName.simpleName()) {
      logger.info("Start")

      val templateType = TypeSpec
        .classBuilder(interfaceName)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .addField(generateTemplateIdField(packageId, interfaceId))
        .addType(
          generateIdClass(
            interfaceName,
            interface.choices,
            packagePrefixes,
          )
        )
        .addType(generateContractClass(interfaceName))
        .build()
      logger.debug("End")
      templateType
    }

  private val idFieldName = "id"
  private val dataFieldName = "data"
  private val agreementFieldName = "agreementText"
  private val signatoriesFieldName = "signatories"
  private val observersFieldName = "observers"

  private val optionalString = ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])
  private def setOfStrings = ParameterizedTypeName.get(classOf[java.util.Set[_]], classOf[String])

  private def generateExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      templateClassName: ClassName,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
    val methodName = s"exercise${choiceName.capitalize}"
    val exerciseChoiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.ExerciseCommand])
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    exerciseChoiceBuilder.addParameter(javaType, "arg")
    choice.param match {
      case TypeCon(_, _) =>
        exerciseChoiceBuilder.addStatement(
          "$T argValue = arg.toValue()",
          classOf[javaapi.data.Value],
        )
      case TypePrim(PrimType.Unit, ImmArraySeq()) =>
        exerciseChoiceBuilder
          .addStatement(
            "$T argValue = $T.getInstance()",
            classOf[javaapi.data.Value],
            classOf[javaapi.data.Unit],
          )
      case TypePrim(_, _) | TypeVar(_) | TypeNumeric(_) =>
        exerciseChoiceBuilder
          .addStatement(
            "$T argValue = new $T(arg)",
            classOf[javaapi.data.Value],
            toAPITypeName(choice.param),
          )
    }
    exerciseChoiceBuilder.addStatement(
      "return new $T($T.TEMPLATE_ID, this.contractId, $S, argValue)",
      classOf[javaapi.data.ExerciseCommand],
      templateClassName,
      choiceName,
    )
    exerciseChoiceBuilder.build()
  }

  private def generateIdClass(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      packagePrefixes: Map[PackageId, String],
  ): TypeSpec = {

    val idClassBuilder =
      TypeSpec
        .classBuilder("ContractId")
        .superclass(
          ParameterizedTypeName
            .get(ClassName.get(classOf[javaapi.data.codegen.ContractId[_]]), templateClassName)
        )
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC)
    val constructor =
      MethodSpec
        .constructorBuilder()
        .addModifiers(Modifier.PUBLIC)
        .addParameter(ClassName.get(classOf[String]), "contractId")
        .addStatement("super(contractId)")
        .build()
    idClassBuilder.addMethod(constructor)
    for ((choiceName, choice) <- choices) {
      val exerciseChoiceMethod =
        generateExerciseMethod(choiceName, choice, templateClassName, packagePrefixes)
      idClassBuilder.addMethod(exerciseChoiceMethod)
    }
    idClassBuilder.build()
  }

  private def generateContractClass(
      templateClassName: ClassName
  ): TypeSpec = {

    val contractIdClassName = ClassName.bestGuess("ContractId")

    val classBuilder =
      TypeSpec.classBuilder("Contract").addModifiers(Modifier.STATIC, Modifier.PUBLIC)

    classBuilder.addField(contractIdClassName, idFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(templateClassName, dataFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(optionalString, agreementFieldName, Modifier.PUBLIC, Modifier.FINAL)

    classBuilder.addField(setOfStrings, signatoriesFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(setOfStrings, observersFieldName, Modifier.PUBLIC, Modifier.FINAL)

    classBuilder.addSuperinterface(ClassName.get(classOf[javaapi.data.Contract]))

    val constructorBuilder = MethodSpec
      .constructorBuilder()
      .addModifiers(Modifier.PUBLIC)
      .addParameter(contractIdClassName, idFieldName)
      .addParameter(templateClassName, dataFieldName)
      .addParameter(optionalString, agreementFieldName)

    constructorBuilder
      .addParameter(setOfStrings, signatoriesFieldName)
      .addParameter(setOfStrings, observersFieldName)

    constructorBuilder.addStatement("this.$L = $L", idFieldName, idFieldName)
    constructorBuilder.addStatement("this.$L = $L", dataFieldName, dataFieldName)
    constructorBuilder.addStatement("this.$L = $L", agreementFieldName, agreementFieldName)
    constructorBuilder.addStatement("this.$L = $L", signatoriesFieldName, signatoriesFieldName)
    constructorBuilder.addStatement("this.$L = $L", observersFieldName, observersFieldName)

    val constructor = constructorBuilder.build()

    classBuilder.addMethod(constructor)

    val contractClassName = ClassName.bestGuess("Contract")
    val fields = Vector(idFieldName, dataFieldName, agreementFieldName)
    classBuilder
//      .addMethod(
//        generateFromIdAndRecord(
//          contractClassName,
//          templateClassName,
//          contractIdClassName,
//        )
//      )
//      .addMethod(
//        generateFromIdAndRecordDeprecated(
//          contractClassName,
//          templateClassName,
//          contractIdClassName,
//        )
//      )
//      .addMethod(
//        generateFromCreatedEvent(
//          contractClassName
//        )
//      )
      .addMethods(
        ObjectMethods(contractClassName, IndexedSeq.empty, fields, templateClassName).asJava
      )
      .build()
  }

//  private[inner] def generateFromIdAndRecord(
//      interfaceName: ClassName,
//      templateClassName: ClassName,
//      idClassName: ClassName,
//  ): MethodSpec = {
//
//    val methodParameters = Iterable(
//      ParameterSpec.builder(classOf[String], "contractId").build(),
//      ParameterSpec.builder(classOf[javaapi.data.DamlRecord], "record$").build(),
//      ParameterSpec.builder(optionalString, agreementFieldName).build(),
//    ) ++ Iterable(
//      ParameterSpec.builder(setOfStrings, signatoriesFieldName).build(),
//      ParameterSpec.builder(setOfStrings, observersFieldName).build(),
//    )
//
//    val spec =
//      MethodSpec
//        .methodBuilder("fromIdAndRecord")
//        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
//        .returns(interfaceName)
//        .addParameters(methodParameters.asJava)
//        .addStatement("$T $L = new $T(contractId)", idClassName, idFieldName, idClassName)
//        .addStatement(
//          "$T $L = $T.fromValue(record$$)",
//          templateClassName,
//          dataFieldName,
//          templateClassName,
//        )
//
//    val callParameterNames =
//      Vector(idFieldName, dataFieldName, agreementFieldName)
//    val callParameters = CodeBlock.join(callParameterNames.map(CodeBlock.of(_)).asJava, ", ")
//    spec.addStatement("return new $T($L)", interfaceName, callParameters).build()
//  }

  private val emptyOptional = CodeBlock.of("$T.empty()", classOf[Optional[_]])
  private val emptySet = CodeBlock.of("$T.emptySet()", classOf[java.util.Collections])

  private[inner] def generateFromIdAndRecordDeprecated(
      interfaceName: ClassName,
      templateClassName: ClassName,
      idClassName: ClassName,
  ): MethodSpec = {
    val spec =
      MethodSpec
        .methodBuilder("fromIdAndRecord")
        .addAnnotation(classOf[Deprecated])
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(interfaceName)
        .addParameter(classOf[String], "contractId")
        .addParameter(classOf[javaapi.data.DamlRecord], "record$")
        .addStatement("$T $L = new $T(contractId)", idClassName, idFieldName, idClassName)
        .addStatement(
          "$T $L = $T.fromValue(record$$)",
          templateClassName,
          dataFieldName,
          templateClassName,
        )

    val callParameters = Vector(
      CodeBlock.of(idFieldName),
      CodeBlock.of(dataFieldName),
      emptyOptional,
    ) ++ Vector(emptySet, emptySet)

    spec
      .addStatement("return new $T($L)", interfaceName, CodeBlock.join(callParameters.asJava, ", "))
      .build()
  }

  private val getContractId = CodeBlock.of("event.getContractId()")
  private val getArguments = CodeBlock.of("event.getArguments()")
  private val getAgreementText = CodeBlock.of("event.getAgreementText()")

  private def generateTemplateIdField(
      packageId: PackageId,
      name: QualifiedName,
  ): FieldSpec = {
    FieldSpec
      .builder(
        ClassName.get(classOf[javaapi.data.Identifier]),
        "TEMPLATE_ID",
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC,
      )
      .initializer(
        "new $T($S, $S, $S)",
        classOf[javaapi.data.Identifier],
        packageId,
        name.module.toString,
        name.name,
      )
      .build()
  }

  private[inner] def generateFromCreatedEvent(
      interfaceName: ClassName
  ) = {

    val spec =
      MethodSpec
        .methodBuilder("fromCreatedEvent")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(interfaceName)
        .addParameter(classOf[javaapi.data.CreatedEvent], "event")

    val params = Vector(getContractId, getArguments, getAgreementText)

    spec.addStatement("return fromIdAndRecord($L)", CodeBlock.join(params.asJava, ", ")).build()
  }

}
