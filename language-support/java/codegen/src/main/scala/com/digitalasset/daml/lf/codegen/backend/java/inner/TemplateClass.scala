// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.TypeWithContext
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
          IdClass
            .builder(
              className,
              template.choices,
              packagePrefixes,
            )
            .addFlattenedExerciseMethods(
              typeWithContext.interface.typeDecls,
              typeWithContext.packageId,
            )
            .addConversionForImplementedInterfaces(template.implementedInterfaces)
            .build()
        )
        .addType(
          ContractIdClass
            .builder(className, template.key, packagePrefixes)
            .addGenerateFromMethods()
            .build()
        )
        .addFields(RecordFields(fields).asJava)
        .addMethods(RecordMethods(fields, className, IndexedSeq.empty, packagePrefixes).asJava)
        .build()
      logger.debug("End")
      templateType
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
              .fold(
                ClassGenUtils.getRecord(_, typeDeclarations, packageId),
                _ => None,
                _ => None,
                _ => None,
              )
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
            .fold(
              ClassGenUtils.getRecord(_, typeDeclarations, packageId),
              _ => None,
              _ => None,
              _ => None,
            )
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
  ): MethodSpec =
    ClassGenUtils.generateFlattenedCreateOrExerciseMethod[javaapi.data.CreateAndExerciseCommand](
      "createAndExercise",
      choiceName,
      choice,
      fields,
      packagePrefixes,
    )

  private def generateTemplateIdField(typeWithContext: TypeWithContext): FieldSpec =
    ClassGenUtils.generateTemplateIdField(
      typeWithContext.packageId,
      typeWithContext.modulesLineage.map(_._1).toImmArray.iterator.mkString("."),
      typeWithContext.name,
    )

}
