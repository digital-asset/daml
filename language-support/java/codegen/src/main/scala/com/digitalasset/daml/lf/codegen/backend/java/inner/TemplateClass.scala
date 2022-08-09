// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import ClassGenUtils.{companionFieldName, templateIdFieldName}
import com.daml.lf.codegen.TypeWithContext
import com.daml.lf.data.Ref, Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.iface._
import com.squareup.javapoet._
import com.typesafe.scalalogging.StrictLogging
import scalaz.{\/, \/-}
import scalaz.syntax.std.option._

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
      logger.info("Start")
      val fields = getFieldsWithTypes(record.fields, packagePrefixes)
      val staticCreateMethod = generateStaticCreateMethod(fields, className)

      val templateChoices = template.tChoices.directChoices
      val templateType = TypeSpec
        .classBuilder(className)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .superclass(classOf[javaapi.data.Template])
        .addField(generateTemplateIdField(typeWithContext))
        .addMethod(generateCreateMethod(className))
        .addMethods(
          generateDeprecatedStaticExerciseByKeyMethods(
            templateChoices,
            template.key,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes,
          )
        )
        .addMethods(
          generateDeprecatedCreateAndExerciseMethods(
            templateChoices,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes,
          )
        )
        .addMethod(staticCreateMethod)
        .addType(
          ContractIdClass
            .builder(
              className,
              templateChoices,
              ContractIdClass.For.Template,
              packagePrefixes,
            )
            .addConversionForImplementedInterfaces(template.implementedInterfaces)
            .addContractIdConversionCompanionForwarder()
            .build()
        )
        .addType(
          ContractClass
            .builder(className, template.key, packagePrefixes)
            .addGenerateFromMethods()
            .build()
        )
        .addType(
          ContractIdClass.generateExercisesInterface(
            templateChoices,
            typeWithContext.interface.typeDecls,
            typeWithContext.packageId,
            packagePrefixes,
          )
        )
        .addMethod(generateCreateAndMethod())
        .addType(generateCreateAndClass(\/-(template.implementedInterfaces)))
        .addField(generateCompanion(className, template.key, packagePrefixes))
        .addFields(RecordFields(fields).asJava)
        .addMethods(RecordMethods(fields, className, IndexedSeq.empty, packagePrefixes).asJava)
      generateByKeyMethod(template.key, packagePrefixes) foreach { byKeyMethod =>
        templateType
          .addMethod(byKeyMethod)
          .addType(generateByKeyClass(\/-(template.implementedInterfaces)))
      }
      logger.debug("End")
      templateType.build()
    }

  private def generateCreateMethod(name: ClassName): MethodSpec =
    MethodSpec
      .methodBuilder("create")
      .addModifiers(Modifier.PUBLIC)
      .addAnnotation(classOf[Override])
      .returns(classOf[javaapi.data.CreateCommand])
      .addStatement(
        "return new $T($T.$N, this.toValue())",
        classOf[javaapi.data.CreateCommand],
        name,
        templateIdFieldName,
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

  private val byKeyClassName = "ByKey"

  private[this] def generateByKeyMethod(
      maybeKey: Option[Type],
      packagePrefixes: Map[PackageId, String],
  ) =
    maybeKey map { key =>
      MethodSpec
        .methodBuilder("byKey")
        .returns(ClassName bestGuess byKeyClassName)
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addParameter(toJavaTypeName(key, packagePrefixes), "key")
        .addStatement(
          "return new ByKey($L)",
          ToValueGenerator
            .generateToValueConverter(key, CodeBlock.of("key"), newNameGenerator, packagePrefixes),
        )
        .addJavadoc(
          """Set up an {@link $T};$Winvoke an {@code exercise} method on the result of
            |this to finish creating the command, or convert to an interface first
            |with {@code toInterface}
            |to invoke an interface {@code exercise} method.""".stripMargin
            .replaceAll("\n", "\\$W"),
          classOf[javaapi.data.ExerciseByKeyCommand],
        )
        .build()
    }

  private[inner] def generateByKeyClass(
      implementedInterfaces: ContractIdClass.For.Interface.type \/ Seq[Ref.TypeConName]
  ) = {
    import scala.language.existentials
    val (superclass, companionArg) = implementedInterfaces.fold(
      (_: ContractIdClass.For.Interface.type) =>
        (classOf[javaapi.data.codegen.ByKey.ToInterface], "companion, "),
      _ => (classOf[javaapi.data.codegen.ByKey], ""),
    )
    TypeSpec
      .classBuilder(byKeyClassName)
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
      .superclass(superclass)
      .addSuperinterface(
        ParameterizedTypeName.get(
          ContractIdClass.exercisesInterface,
          ClassName get classOf[javaapi.data.ExerciseByKeyCommand],
        )
      )
      .addMethod(
        MethodSpec
          .constructorBuilder()
          .publicIfInterface(implementedInterfaces)
          .companionIfInterface(implementedInterfaces)
          .addParameter(classOf[javaapi.data.Value], "key")
          .addStatement("super($Lkey)", companionArg)
          .build()
      )
      .addGetCompanion(implementedInterfaces)
      .addMethods(
        implementedInterfaces
          .fold(
            (_: ContractIdClass.For.Interface.type) => Seq.empty,
            implemented =>
              ContractIdClass
                .generateToInterfaceMethods(
                  byKeyClassName,
                  s"$companionFieldName, this.contractKey",
                  implemented,
                ),
          )
          .asJava
      )
      .build()
  }

  // TODO #14039 delete
  private def generateDeprecatedStaticExerciseByKeyMethods(
      choices: Map[ChoiceName, TemplateChoice[Type]],
      maybeKey: Option[Type],
      typeDeclarations: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
      packagePrefixes: Map[PackageId, String],
  ) =
    maybeKey.fold(java.util.Collections.emptyList[MethodSpec]()) { key =>
      val methods = for ((choiceName, choice) <- choices.toList) yield {
        val raw = generateDeprecatedStaticExerciseByKeyMethod(
          choiceName,
          choice,
          key,
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
              generateDeprecatedFlattenedStaticExerciseByKeyMethod(
                choiceName,
                key,
                getFieldsWithTypes(record.fields, packagePrefixes),
                packagePrefixes,
              )
            }
        raw :: flattened.toList
      }
      methods.flatten.asJava
    }

  // TODO #14039 delete
  private def generateDeprecatedStaticExerciseByKeyMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      key: Type,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec =
    MethodSpec
      .methodBuilder(s"exerciseByKey${choiceName.capitalize}")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(classOf[javaapi.data.ExerciseByKeyCommand])
      .makeDeprecated(
        howToFix = s"use {@code byKey(key).exercise${choiceName.capitalize}} instead",
        sinceDaml = "2.3.0",
      )
      .addParameter(toJavaTypeName(key, packagePrefixes), "key")
      .addParameter(toJavaTypeName(choice.param, packagePrefixes), "arg")
      .addStatement(
        "return byKey(key).exercise$L(arg)",
        choiceName.capitalize,
      )
      .build()

  // TODO #14039 delete
  private def generateDeprecatedFlattenedStaticExerciseByKeyMethod(
      choiceName: ChoiceName,
      key: Type,
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
    val methodName = s"exerciseByKey${choiceName.capitalize}"
    val exerciseByKeyBuilder = MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(classOf[javaapi.data.ExerciseByKeyCommand])
      .addParameter(toJavaTypeName(key, packagePrefixes), "key")
    for (FieldInfo(_, _, javaName, javaType) <- fields) {
      exerciseByKeyBuilder.addParameter(javaType, javaName)
    }
    val expansion = CodeBlock.of(
      "byKey(key).exercise$L($L)",
      choiceName.capitalize,
      generateArgumentList(fields.map(_.javaName)),
    )
    exerciseByKeyBuilder
      .makeDeprecated(CodeBlock.of("use {@code $L} instead", expansion), sinceDaml = "2.3.0")
      .addStatement("return $L", expansion)
      .build()
  }

  private val createAndClassName = "CreateAnd"

  private[this] def generateCreateAndMethod() =
    MethodSpec
      .methodBuilder("createAnd")
      .returns(ClassName bestGuess createAndClassName)
      .addModifiers(Modifier.PUBLIC)
      .addAnnotation(classOf[Override])
      .addStatement("return new CreateAnd(this)")
      .build()

  private[inner] def generateCreateAndClass(
      implementedInterfaces: ContractIdClass.For.Interface.type \/ Seq[Ref.TypeConName]
  ) = {
    import scala.language.existentials
    val (superclass, companionArg) = implementedInterfaces.fold(
      (_: ContractIdClass.For.Interface.type) =>
        (classOf[javaapi.data.codegen.CreateAnd.ToInterface], "companion, "),
      _ => (classOf[javaapi.data.codegen.CreateAnd], ""),
    )
    TypeSpec
      .classBuilder(createAndClassName)
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
      .superclass(superclass)
      .addSuperinterface(
        ParameterizedTypeName.get(
          ContractIdClass.exercisesInterface,
          ClassName get classOf[javaapi.data.CreateAndExerciseCommand],
        )
      )
      .addGetCompanion(implementedInterfaces)
      .addMethod(
        MethodSpec
          .constructorBuilder()
          .publicIfInterface(implementedInterfaces)
          .companionIfInterface(implementedInterfaces)
          .addParameter(classOf[javaapi.data.Template], "createArguments")
          .addStatement("super($LcreateArguments)", companionArg)
          .build()
      )
      .addMethods(
        implementedInterfaces
          .fold(
            (_: ContractIdClass.For.Interface.type) => Seq.empty,
            implemented =>
              ContractIdClass
                .generateToInterfaceMethods(
                  createAndClassName,
                  s"$companionFieldName, this.createArguments",
                  implemented,
                ),
          )
          .asJava
      )
      .build()
  }

  // TODO #14039 delete
  private def generateDeprecatedCreateAndExerciseMethods(
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      typeDeclarations: Map[QualifiedName, InterfaceType],
      packageId: PackageId,
      packagePrefixes: Map[PackageId, String],
  ) = {
    val methods = for ((choiceName, choice) <- choices) yield {
      val createAndExerciseChoiceMethod =
        generateDeprecatedCreateAndExerciseMethod(
          choiceName,
          choice,
          packagePrefixes,
        )
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
          generateDeprecatedFlattenedCreateAndExerciseMethod(
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

  // TODO #14039 delete
  private def generateDeprecatedCreateAndExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec = {
    val methodName = s"createAndExercise${choiceName.capitalize}"
    val javaType = toJavaTypeName(choice.param, packagePrefixes)
    MethodSpec
      .methodBuilder(methodName)
      .addModifiers(Modifier.PUBLIC)
      .makeDeprecated(
        howToFix = s"use {@code createAnd().exercise${choiceName.capitalize}} instead",
        sinceDaml = "2.3.0",
      )
      .returns(classOf[javaapi.data.CreateAndExerciseCommand])
      .addParameter(javaType, "arg")
      .addStatement(
        "return createAnd().exercise$L(arg)",
        choiceName.capitalize,
      )
      .build()
  }

  // TODO #14039 delete
  private def generateDeprecatedFlattenedCreateAndExerciseMethod(
      choiceName: ChoiceName,
      choice: TemplateChoice[Type],
      fields: Fields,
      packagePrefixes: Map[PackageId, String],
  ): MethodSpec =
    ClassGenUtils.generateFlattenedCreateOrExerciseMethod(
      "createAndExercise",
      ClassName get classOf[javaapi.data.CreateAndExerciseCommand],
      choiceName,
      choice,
      fields,
      packagePrefixes,
    )(
      _.makeDeprecated(
        howToFix = s"use {@code createAnd().exercise${choiceName.capitalize}} instead",
        sinceDaml = "2.3.0",
      )
    )

  private def generateTemplateIdField(typeWithContext: TypeWithContext): FieldSpec =
    ClassGenUtils.generateTemplateIdField(
      typeWithContext.packageId,
      typeWithContext.modulesLineage.map(_._1).toImmArray.iterator.mkString("."),
      typeWithContext.name,
    )

  private def generateCompanion(
      templateClassName: ClassName,
      maybeKey: Option[Type],
      packagePrefixes: Map[PackageId, String],
  ): FieldSpec = {
    import scala.language.existentials
    import javaapi.data.codegen.ContractCompanion
    val (fieldClass, keyTypes, keyParams, keyArgs) = maybeKey.cata(
      keyType =>
        (
          classOf[ContractCompanion.WithKey[_, _, _, _]],
          Seq(toJavaTypeName(keyType, packagePrefixes)),
          ",$We -> $L",
          Seq(
            FromValueGenerator
              .extractor(keyType, "e", CodeBlock.of("e"), newNameGenerator, packagePrefixes)
          ),
        ),
      (classOf[ContractCompanion.WithoutKey[_, _, _]], Seq.empty, "", Seq.empty),
    )
    val contractIdName = ClassName bestGuess "ContractId"
    val contractName = ClassName bestGuess "Contract"
    FieldSpec
      .builder(
        ParameterizedTypeName.get(
          ClassName get fieldClass,
          Seq(
            contractName,
            contractIdName,
            templateClassName,
          ) ++ keyTypes: _*
        ),
        companionFieldName,
        Modifier.STATIC,
        Modifier.FINAL,
        Modifier.PUBLIC,
      )
      .initializer(
        "$Znew $T<>($>$Z$S,$W$N, $T::new, $T::fromValue, $T::new" + keyParams + "$<)",
        Seq(
          fieldClass,
          templateClassName,
          templateIdFieldName,
          contractIdName,
          templateClassName,
          contractName,
        ) ++ keyArgs: _*
      )
      .build()
  }

  private implicit final class `MethodSpec extensions`(private val self: MethodSpec.Builder)
      extends AnyVal {
    // for template, use createAnd() or byKey(); toInterface methods need public
    // access if in different packages, though
    private[TemplateClass] def publicIfInterface(
        isInterface: ContractIdClass.For.Interface.type \/ _
    ) =
      self.addModifiers(
        isInterface.fold(_ => Some(Modifier.PUBLIC), _ => None).toList.asJava
      )

    private[TemplateClass] def companionIfInterface(
        isInterface: ContractIdClass.For.Interface.type \/ _
    ) =
      isInterface.fold(
        { _ =>
          val wildcard = WildcardTypeName subtypeOf classOf[Object]
          self.addParameter(
            ParameterizedTypeName.get(
              ClassName get classOf[javaapi.data.codegen.ContractCompanion[_, _, _]],
              wildcard,
              wildcard,
              wildcard,
            ),
            "companion",
          )
        },
        _ => self,
      )

    private[TemplateClass] def makeDeprecated(
        howToFix: String,
        sinceDaml: String,
    ): MethodSpec.Builder =
      self.makeDeprecated(howToFix = CodeBlock.of("$L", howToFix), sinceDaml = sinceDaml)

    private[TemplateClass] def makeDeprecated(
        howToFix: CodeBlock,
        sinceDaml: String,
    ): MethodSpec.Builder =
      self
        .addAnnotation(classOf[Deprecated])
        .addJavadoc(
          "@deprecated since Daml $L; $L",
          sinceDaml,
          howToFix,
        )
  }

  private implicit final class `TypeSpec extensions`(private val self: TypeSpec.Builder)
      extends AnyVal {
    private[TemplateClass] def addGetCompanion(
        isInterface: ContractIdClass.For.Interface.type \/ _
    ) =
      self.addMethod(
        ContractIdClass.Builder.generateGetCompanion(
          isInterface.map(_ => ContractIdClass.For.Template).merge
        )
      )
  }
}
