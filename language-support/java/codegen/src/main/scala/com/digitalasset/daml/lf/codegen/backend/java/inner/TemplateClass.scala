// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import java.util.Optional

import com.daml.ledger.javaapi
import com.daml.lf.codegen.TypeWithContext
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.iface._
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import javax.lang.model.element.Modifier

import scala.jdk.CollectionConverters._

private[inner] object TemplateClass extends StrictLogging {

  def generate(
      className: ClassName,
      record: Record.FWT,
      template: DefTemplate.FWT,
      typeWithContext: TypeWithContext,
      packagePrefixes: Map[PackageId, String],
  ): TypeSpec =
    TrackLineage.of("template", typeWithContext.name) {
      val fields = getFieldsWithTypes(record.fields, packagePrefixes)
      logger.info("Start")
      val staticCreateMethod = generateStaticCreateMethod(fields, className)

      val templateType = TypeSpec
        .classBuilder(className)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .superclass(classOf[javaapi.data.Template])
        .addField(generateTemplateIdField(typeWithContext))
        .addMethod(generateCreateMethod(className))
        .addMethods(
          generateStaticExerciseByKeyMethods(
            className,
            template.choices,
            template.key,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes,
          )
        )
        .addMethods(
          generateCreateAndExerciseMethods(
            className,
            template.choices,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes,
          )
        )
        .addMethod(staticCreateMethod)
        .addType(
          generateIdClass(
            className,
            template.choices,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes,
          )
        )
        .addType(generateContractClass(className, template.key, packagePrefixes))
        .addFields(RecordFields(fields).asJava)
        .addMethods(RecordMethods(fields, className, IndexedSeq.empty, packagePrefixes).asJava)
        .build()
      logger.debug("End")
      templateType
    }

  private val idFieldName = "id"
  private val dataFieldName = "data"
  private val agreementFieldName = "agreementText"
  private val contractKeyFieldName = "key"
  private val signatoriesFieldName = "signatories"
  private val observersFieldName = "observers"

  private val optionalString = ParameterizedTypeName.get(classOf[Optional[_]], classOf[String])
  private def optional(name: TypeName) =
    ParameterizedTypeName.get(ClassName.get(classOf[Optional[_]]), name)
  private def setOfStrings = ParameterizedTypeName.get(classOf[java.util.Set[_]], classOf[String])

  private def generateContractClass(
      templateClassName: ClassName,
      key: Option[Type],
      packagePrefixes: Map[PackageId, String],
  ): TypeSpec = {

    val contractIdClassName = ClassName.bestGuess("ContractId")
    val contractKeyClassName = key.map(toJavaTypeName(_, packagePrefixes))

    val classBuilder =
      TypeSpec.classBuilder("Contract").addModifiers(Modifier.STATIC, Modifier.PUBLIC)

    classBuilder.addField(contractIdClassName, idFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(templateClassName, dataFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(optionalString, agreementFieldName, Modifier.PUBLIC, Modifier.FINAL)

    contractKeyClassName.foreach { name =>
      classBuilder.addField(optional(name), contractKeyFieldName, Modifier.PUBLIC, Modifier.FINAL)
    }

    classBuilder.addField(setOfStrings, signatoriesFieldName, Modifier.PUBLIC, Modifier.FINAL)
    classBuilder.addField(setOfStrings, observersFieldName, Modifier.PUBLIC, Modifier.FINAL)

    classBuilder.addSuperinterface(ClassName.get(classOf[javaapi.data.Contract]))

    val constructorBuilder = MethodSpec
      .constructorBuilder()
      .addModifiers(Modifier.PUBLIC)
      .addParameter(contractIdClassName, idFieldName)
      .addParameter(templateClassName, dataFieldName)
      .addParameter(optionalString, agreementFieldName)

    contractKeyClassName.foreach { name =>
      constructorBuilder.addParameter(optional(name), contractKeyFieldName)
    }

    constructorBuilder
      .addParameter(setOfStrings, signatoriesFieldName)
      .addParameter(setOfStrings, observersFieldName)

    constructorBuilder.addStatement("this.$L = $L", idFieldName, idFieldName)
    constructorBuilder.addStatement("this.$L = $L", dataFieldName, dataFieldName)
    constructorBuilder.addStatement("this.$L = $L", agreementFieldName, agreementFieldName)
    contractKeyClassName.foreach { _ =>
      constructorBuilder.addStatement("this.$L = $L", contractKeyFieldName, contractKeyFieldName)
    }
    constructorBuilder.addStatement("this.$L = $L", signatoriesFieldName, signatoriesFieldName)
    constructorBuilder.addStatement("this.$L = $L", observersFieldName, observersFieldName)

    val constructor = constructorBuilder.build()

    classBuilder.addMethod(constructor)

    val contractClassName = ClassName.bestGuess("Contract")
    val fields = Vector(idFieldName, dataFieldName, agreementFieldName) ++ contractKeyClassName
      .map(_ => contractKeyFieldName)
      .toList ++ Vector(signatoriesFieldName, observersFieldName)
    classBuilder
      .addMethod(
        generateFromIdAndRecord(
          contractClassName,
          templateClassName,
          contractIdClassName,
          contractKeyClassName,
        )
      )
      .addMethod(
        generateFromIdAndRecordDeprecated(
          contractClassName,
          templateClassName,
          contractIdClassName,
          contractKeyClassName,
        )
      )
      .addMethod(
        generateFromCreatedEvent(
          contractClassName,
          key,
          packagePrefixes,
        )
      )
      .addMethods(
        ObjectMethods(contractClassName, IndexedSeq.empty, fields, templateClassName).asJava
      )
      .build()
  }

  private[inner] def generateFromIdAndRecord(
      className: ClassName,
      templateClassName: ClassName,
      idClassName: ClassName,
      maybeContractKeyClassName: Option[TypeName],
  ): MethodSpec = {

    val methodParameters = Iterable(
      ParameterSpec.builder(classOf[String], "contractId").build(),
      ParameterSpec.builder(classOf[javaapi.data.DamlRecord], "record$").build(),
      ParameterSpec.builder(optionalString, agreementFieldName).build(),
    ) ++ maybeContractKeyClassName
      .map(name => ParameterSpec.builder(optional(name), contractKeyFieldName).build)
      .toList ++ Iterable(
      ParameterSpec.builder(setOfStrings, signatoriesFieldName).build(),
      ParameterSpec.builder(setOfStrings, observersFieldName).build(),
    )

    val spec =
      MethodSpec
        .methodBuilder("fromIdAndRecord")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(className)
        .addParameters(methodParameters.asJava)
        .addStatement("$T $L = new $T(contractId)", idClassName, idFieldName, idClassName)
        .addStatement(
          "$T $L = $T.fromValue(record$$)",
          templateClassName,
          dataFieldName,
          templateClassName,
        )

    val callParameterNames =
      Vector(idFieldName, dataFieldName, agreementFieldName) ++ maybeContractKeyClassName
        .map(_ => contractKeyFieldName)
        .toList ++ Vector(signatoriesFieldName, observersFieldName).toList
    val callParameters = CodeBlock.join(callParameterNames.map(CodeBlock.of(_)).asJava, ", ")
    spec.addStatement("return new $T($L)", className, callParameters).build()
  }

  private val emptyOptional = CodeBlock.of("$T.empty()", classOf[Optional[_]])
  private val emptySet = CodeBlock.of("$T.emptySet()", classOf[java.util.Collections])

  private[inner] def generateFromIdAndRecordDeprecated(
      className: ClassName,
      templateClassName: ClassName,
      idClassName: ClassName,
      maybeContractKeyClassName: Option[TypeName],
  ): MethodSpec = {
    val spec =
      MethodSpec
        .methodBuilder("fromIdAndRecord")
        .addAnnotation(classOf[Deprecated])
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(className)
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
    ) ++ maybeContractKeyClassName.map(_ => emptyOptional).toList ++ Vector(emptySet, emptySet)

    spec
      .addStatement("return new $T($L)", className, CodeBlock.join(callParameters.asJava, ", "))
      .build()
  }

  private val getContractId = CodeBlock.of("event.getContractId()")
  private val getArguments = CodeBlock.of("event.getArguments()")
  private val getAgreementText = CodeBlock.of("event.getAgreementText()")
  private def getContractKey(t: Type, packagePrefixes: Map[PackageId, String]) =
    CodeBlock.of(
      "event.getContractKey().map(e -> $L)",
      FromValueGenerator.extractor(t, "e", CodeBlock.of("e"), newNameGenerator, packagePrefixes),
    )
  private val getSignatories = CodeBlock.of("event.getSignatories()")
  private val getObservers = CodeBlock.of("event.getObservers()")

  private[inner] def generateFromCreatedEvent(
      className: ClassName,
      maybeContractKeyType: Option[Type],
      packagePrefixes: Map[PackageId, String],
  ) = {

    val spec =
      MethodSpec
        .methodBuilder("fromCreatedEvent")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(className)
        .addParameter(classOf[javaapi.data.CreatedEvent], "event")

    val params = Vector(getContractId, getArguments, getAgreementText) ++ maybeContractKeyType
      .map(getContractKey(_, packagePrefixes))
      .toList ++ Vector(getSignatories, getObservers)

    spec.addStatement("return fromIdAndRecord($L)", CodeBlock.join(params.asJava, ", ")).build()
  }

  private def generateCreateMethod(name: ClassName): MethodSpec =
    MethodSpec
      .methodBuilder("create")
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.CreateCommand])
      .addStatement(
        "return new $T($T.TEMPLATE_ID, this.toValue())",
        classOf[javaapi.data.CreateCommand],
        name,
      )
      .build()

  private def generateStaticCreateMethod(fields: Fields, name: ClassName): MethodSpec =
    fields
      .foldLeft(
        MethodSpec
          .methodBuilder("create")
          .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
          .returns(classOf[javaapi.data.CreateCommand])
      ) { case (b, FieldInfo(_, _, escapedName, tpe)) =>
        b.addParameter(tpe, escapedName)
      }
      .addStatement(
        "return new $T($L).create()",
        name,
        generateArgumentList(fields.map(_.javaName)),
      )
      .build()

  private def generateIdClass(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      typeDeclarations: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
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
      for (
        record <- choice.param.fold(
          getRecord(_, typeDeclarations, packageId),
          _ => None,
          _ => None,
          _ => None,
        )
      ) {
        val splatted = generateFlattenedExerciseMethod(
          choiceName,
          choice,
          getFieldsWithTypes(record.fields, packagePrefixes),
          packagePrefixes,
        )
        idClassBuilder.addMethod(splatted)
      }
    }
    idClassBuilder.build()
  }

  private def getRecord(
      typeCon: TypeCon,
      identifierToType: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
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

  private def generateStaticExerciseByKeyMethods(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[Type]],
      maybeKey: Option[Type],
      typeDeclarations: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
      packagePrefixes: Map[PackageId, String],
  ) =
    maybeKey.fold(java.util.Collections.emptyList[MethodSpec]()) { key =>
      val methods = for ((choiceName, choice) <- choices.toList) yield {
        val raw = generateStaticExerciseByKeyMethod(
          choiceName,
          choice,
          key,
          templateClassName,
          packagePrefixes,
        )
        val flattened =
          for (
            record <- choice.param
              .fold(getRecord(_, typeDeclarations, packageId), _ => None, _ => None, _ => None)
          )
            yield {
              generateFlattenedStaticExerciseByKeyMethod(
                choiceName,
                choice,
                key,
                templateClassName,
                getFieldsWithTypes(record.fields, packagePrefixes),
                packagePrefixes,
              )
            }
        raw :: flattened.toList
      }
      methods.flatten.asJava
    }

  private def generateStaticExerciseByKeyMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      key: Type,
      templateClassName: ClassName,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
    val exerciseByKeyBuilder = MethodSpec
      .methodBuilder(s"exerciseByKey${choiceName.capitalize}")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(classOf[javaapi.data.ExerciseByKeyCommand])
    val keyJavaType = toJavaTypeName(key, packagePrefixes)
    exerciseByKeyBuilder.addParameter(keyJavaType, "key")
    val choiceJavaType = toJavaTypeName(choice.param, packagePrefixes)
    exerciseByKeyBuilder.addParameter(choiceJavaType, "arg")
    val choiceArgument = choice.param match {
      case TypeCon(_, _) => "arg.toValue()"
      case TypePrim(_, _) | TypeVar(_) | TypeNumeric(_) => "arg"
    }
    exerciseByKeyBuilder.addStatement(
      "return new $T($T.TEMPLATE_ID, $L, $S, $L)",
      classOf[javaapi.data.ExerciseByKeyCommand],
      templateClassName,
      ToValueGenerator
        .generateToValueConverter(key, CodeBlock.of("key"), newNameGenerator, packagePrefixes),
      choiceName,
      choiceArgument,
    )
    exerciseByKeyBuilder.build()
  }

  private def generateFlattenedStaticExerciseByKeyMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      key: Type,
      templateClassName: ClassName,
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
    val methodName = s"exerciseByKey${choiceName.capitalize}"
    val exerciseByKeyBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(classOf[javaapi.data.ExerciseByKeyCommand])
    val keyJavaType = toJavaTypeName(key, packagePrefixes)
    exerciseByKeyBuilder.addParameter(keyJavaType, "key")
    val choiceJavaType = toJavaTypeName(choice.param, packagePrefixes)
    for (FieldInfo(_, _, javaName, javaType) <- fields) {
      exerciseByKeyBuilder.addParameter(javaType, javaName)
    }
    exerciseByKeyBuilder.addStatement(
      "return $T.$L(key, new $T($L))",
      templateClassName,
      methodName,
      choiceJavaType,
      generateArgumentList(fields.map(_.javaName)),
    )
    exerciseByKeyBuilder.build()
  }

  private def generateCreateAndExerciseMethods(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      typeDeclarations: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
      packagePrefixes: Map[PackageId, String],
  ) = {
    val methods = for ((choiceName, choice) <- choices) yield {
      val createAndExerciseChoiceMethod =
        generateCreateAndExerciseMethod(choiceName, choice, templateClassName, packagePrefixes)
      val splatted =
        for (
          record <- choice.param
            .fold(getRecord(_, typeDeclarations, packageId), _ => None, _ => None, _ => None)
        ) yield {
          generateFlattenedCreateAndExerciseMethod(
            choiceName,
            choice,
            getFieldsWithTypes(record.fields, packagePrefixes),
            packagePrefixes,
          )
        }
      createAndExerciseChoiceMethod :: splatted.toList
    }
    methods.flatten.asJava
  }

  private def generateCreateAndExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      templateClassName: ClassName,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
    val methodName = s"createAndExercise${choiceName.capitalize}"
    val createAndExerciseChoiceBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .returns(classOf[javaapi.data.CreateAndExerciseCommand])
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    createAndExerciseChoiceBuilder.addParameter(javaType, "arg")
    choice.param match {
      case TypeCon(_, _) =>
        createAndExerciseChoiceBuilder.addStatement(
          "$T argValue = arg.toValue()",
          classOf[javaapi.data.Value],
        )
      case TypePrim(PrimType.Unit, ImmArraySeq()) =>
        createAndExerciseChoiceBuilder
          .addStatement(
            "$T argValue = $T.getInstance()",
            classOf[javaapi.data.Value],
            classOf[javaapi.data.Unit],
          )
      case TypePrim(_, _) | TypeVar(_) | TypeNumeric(_) =>
        createAndExerciseChoiceBuilder
          .addStatement(
            "$T argValue = new $T(arg)",
            classOf[javaapi.data.Value],
            toAPITypeName(choice.param),
          )
    }
    createAndExerciseChoiceBuilder.addStatement(
      "return new $T($T.TEMPLATE_ID, this.toValue(), $S, argValue)",
      classOf[javaapi.data.CreateAndExerciseCommand],
      templateClassName,
      choiceName,
    )
    createAndExerciseChoiceBuilder.build()
  }

  private def generateFlattenedCreateAndExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
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
      generateArgumentList(fields.map(_.javaName)),
    )
    createAndExerciseChoiceBuilder.build()
  }

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

  private def generateFlattenedExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
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
      generateArgumentList(fields.map(_.javaName)),
    )
    exerciseChoiceBuilder.build()
  }

  private def generateTemplateIdField(typeWithContext: TypeWithContext): FieldSpec =
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
        typeWithContext.packageId,
        typeWithContext.modulesLineage.map(_._1).toImmArray.iterator.mkString("."),
        typeWithContext.name,
      )
      .build()

}
