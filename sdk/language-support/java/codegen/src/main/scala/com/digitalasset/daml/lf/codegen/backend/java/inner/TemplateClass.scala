// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import ClassGenUtils.{companionFieldName, generateGetCompanion, templateIdFieldName}
import com.digitalasset.daml.lf.codegen.TypeWithContext
import com.digitalasset.daml.lf.data.Ref
import Ref.ChoiceName
import com.daml.ledger.javaapi.data.codegen.{Choice, Created, Update, ContractTypeCompanion}
import com.digitalasset.daml.lf.codegen.backend.java.inner.ToValueGenerator.generateToValueConverter
import com.digitalasset.daml.lf.typesig
import typesig._
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
  )(implicit
      packagePrefixes: PackagePrefixes
  ): (TypeSpec, Seq[(ClassName, String)]) =
    TrackLineage.of("template", typeWithContext.name) {
      logger.info("Start")
      val fields = getFieldsWithTypes(record.fields)
      val staticCreateMethod = generateStaticCreateMethod(fields, className)

      val templateChoices = template.tChoices.directChoices
      val companion = new Companion(className, template.key)

      val (templateMethods, staticImports) = TemplateMethods(fields, className)

      val templateType = TypeSpec
        .classBuilder(className)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .superclass(classOf[javaapi.data.Template])
        .addFields(generateTemplateIdFields(typeWithContext).asJava)
        .addField(ClassGenUtils.generatePackageIdField(typeWithContext.packageId))
        .addField(generatePackageNameField(typeWithContext))
        .addField(generatePackageVersionField(typeWithContext))
        .addMethod(generateCreateMethod(className))
        .addMethod(staticCreateMethod)
        .addType(
          ContractIdClass
            .builder(
              className,
              templateChoices,
              ContractIdClass.For.Template,
            )
            .addConversionForImplementedInterfaces(template.implementedInterfaces)
            .addContractIdConversionCompanionForwarder()
            .build()
        )
        .addType(
          ContractClass
            .builder(className, template.key)
            .addGenerateFromMethods()
            .build()
        )
        .addType(
          ContractIdClass.generateExercisesInterface(
            className,
            templateChoices,
            typeWithContext.auxiliarySignatures,
          )
        )
        .addMethod(generateCreateAndMethod(className))
        .addType(
          generateCreateAndClass(className, Right(template.implementedInterfaces))
        )
        .addFields(
          generateChoicesMetadata(
            className: ClassName,
            templateChoices,
          ).asJava
        )
        .addField(companion.generateField(templateChoices.keySet))
        .addMethod(companion.generateGetter())
        .addFields(RecordFields(fields).asJava)
        .addMethods(templateMethods.asJava)
        .addType(FromJsonGenerator.decoderAccessorClass(className, Vector()))
      generateByKeyMethod(className, template.key) foreach { byKeyMethod =>
        templateType
          .addMethod(byKeyMethod)
          .addType(
            generateByKeyClass(className, Right(template.implementedInterfaces))
          )
      }
      logger.debug("End")
      (templateType.build(), staticImports)
    }

  private val updateClassName = ClassName get classOf[Update[_]]
  private val createUpdateClassName = ClassName get classOf[Update.CreateUpdate[_, _]]
  private val createdClassName = ClassName get classOf[Created[_]]
  private def parameterizedTypeName(raw: ClassName, arg: TypeName*) =
    ParameterizedTypeName.get(raw, arg: _*)

  private def generateCreateMethod(name: ClassName): MethodSpec = {
    val createdType = parameterizedTypeName(createdClassName, nestedClassName(name, "ContractId"))
    val contractIdClassName = nestedClassName(name, "ContractId")
    MethodSpec
      .methodBuilder("create")
      .addModifiers(Modifier.PUBLIC)
      .addAnnotation(classOf[Override])
      .returns(
        parameterizedTypeName(updateClassName, createdType)
      )
      .addStatement(
        "return new $T(new $T($T.$N, this.toValue()), x -> x, $T::new)",
        parameterizedTypeName(createUpdateClassName, contractIdClassName, createdType),
        classOf[javaapi.data.CreateCommand],
        name,
        templateIdFieldName,
        contractIdClassName,
      )
      .build()
  }

  private def generateStaticCreateMethod(fields: Fields, name: ClassName): MethodSpec =
    fields
      .foldLeft(
        MethodSpec
          .methodBuilder("create")
          .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
          .returns(
            parameterizedTypeName(
              updateClassName,
              parameterizedTypeName(createdClassName, nestedClassName(name, "ContractId")),
            )
          )
      ) { case (b, FieldInfo(_, _, escapedName, tpe)) =>
        b.addParameter(tpe, escapedName)
      }
      .addStatement(
        "return new $T($L).create()",
        name,
        generateArgumentList(fields.map(_.javaName)),
      )
      .build()

  private[this] def generateByKeyMethod(className: ClassName, maybeKey: Option[Type])(implicit
      packagePrefixes: PackagePrefixes
  ) =
    maybeKey map { key =>
      MethodSpec
        .methodBuilder("byKey")
        .returns(nestedClassName(className, "ByKey"))
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addParameter(toJavaTypeName(key), "key")
        .addStatement(
          "return new ByKey($L)",
          generateToValueConverter(key, CodeBlock.of("key"), newNameGenerator),
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
      markerName: ClassName,
      implementedInterfaces: Either[ContractIdClass.For.Interface.type, Seq[Ref.TypeConId]],
  )(implicit
      packagePrefixes: PackagePrefixes
  ) = {
    import scala.language.existentials
    val (superclass, companionArg) = implementedInterfaces.fold(
      (_: ContractIdClass.For.Interface.type) =>
        (classOf[javaapi.data.codegen.ByKey.ToInterface], "companion, "),
      _ => (classOf[javaapi.data.codegen.ByKey], ""),
    )
    val byKeyClassName = nestedClassName(markerName, "ByKey").simpleName
    TypeSpec
      .classBuilder(byKeyClassName)
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
      .superclass(superclass)
      .addSuperinterface(
        ParameterizedTypeName.get(
          ContractIdClass.exercisesInterface(markerName),
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
      .addGetCompanion(markerName, implementedInterfaces)
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

  private[this] def generateCreateAndMethod(className: ClassName) = {
    val createAndClassName = nestedClassName(className, "CreateAnd")
    MethodSpec
      .methodBuilder("createAnd")
      .returns(createAndClassName)
      .addModifiers(Modifier.PUBLIC)
      .addAnnotation(classOf[Override])
      .addStatement("return new $T(this)", createAndClassName)
      .build()
  }

  private[inner] def generateCreateAndClass(
      markerName: ClassName,
      implementedInterfaces: Either[ContractIdClass.For.Interface.type, Seq[Ref.TypeConId]],
  )(implicit
      packagePrefixes: PackagePrefixes
  ) = {
    import scala.language.existentials
    val (superclass, companionArg) = implementedInterfaces.fold(
      (_: ContractIdClass.For.Interface.type) =>
        (classOf[javaapi.data.codegen.CreateAnd.ToInterface], "companion, "),
      _ => (classOf[javaapi.data.codegen.CreateAnd], ""),
    )
    val createAndClassName = nestedClassName(markerName, "CreateAnd")
    TypeSpec
      .classBuilder(createAndClassName)
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
      .superclass(superclass)
      .addSuperinterface(
        ParameterizedTypeName.get(
          ContractIdClass.exercisesInterface(markerName),
          ClassName get classOf[javaapi.data.CreateAndExerciseCommand],
        )
      )
      .addGetCompanion(markerName, implementedInterfaces)
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
                  createAndClassName.simpleName,
                  s"$companionFieldName, this.createArguments",
                  implemented,
                ),
          )
          .asJava
      )
      .build()
  }

  private def generateTemplateIdFields(typeWithContext: TypeWithContext): Seq[FieldSpec] =
    ClassGenUtils.generateTemplateIdFields(
      typeWithContext.packageId,
      typeWithContext.signature.metadata.name,
      typeWithContext.modulesLineage.map(_._1).toImmArray.iterator.mkString("."),
      typeWithContext.name,
    )

  private def generatePackageVersionField(typeWithContext: TypeWithContext) =
    ClassGenUtils.generatePackageVersionField(typeWithContext.signature.metadata.version)

  private def generatePackageNameField(typeWithContext: TypeWithContext) =
    ClassGenUtils.generatePackageNameField(typeWithContext.signature.metadata.name)

  def generateChoicesMetadata(
      templateClassName: ClassName,
      templateChoices: Map[ChoiceName, TemplateChoice.FWT],
  )(implicit packagePrefixes: PackagePrefixes): Seq[FieldSpec] = {
    templateChoices.map { case (choiceName, choice) =>
      val fieldClass = classOf[Choice[_, _, _]]
      print(fieldClass)
      FieldSpec
        .builder(
          ParameterizedTypeName.get(
            ClassName get fieldClass,
            templateClassName,
            toJavaTypeName(choice.param),
            toJavaTypeName(choice.returnType),
          ),
          toChoiceNameField(choiceName),
          Modifier.STATIC,
          Modifier.FINAL,
          Modifier.PUBLIC,
        )
        .initializer(
          "$Z$T.create($>$S, value$$ -> $L,$W$L,$Wvalue$$ ->$W$L,$W$L,$W$L,$W$L,$W$L)$<",
          fieldClass,
          choiceName,
          generateToValueConverter(
            choice.param,
            CodeBlock.of("value$$"),
            Iterator.empty,
          ),
          FromValueGenerator.extractor(
            choice.param,
            "value$",
            CodeBlock.of("$L", "value$"),
            newNameGenerator,
          ),
          CodeBlock.of("$T.valueDecoder()", templateClassName),
          FromJsonGenerator.jsonDecoderForType(choice.param),
          FromJsonGenerator.jsonDecoderForType(choice.returnType),
          ToJsonGenerator.encoderOf(choice.param),
          ToJsonGenerator.encoderOf(choice.returnType),
        )
        .build()
    }.toSeq
  }

  private final class Companion(templateClassName: ClassName, maybeKey: Option[Type])(implicit
      packagePrefixes: PackagePrefixes
  ) {
    import scala.language.existentials
    import javaapi.data.codegen.ContractCompanion

    private val (fieldClass, keyTypes, keyParams, keyArgs) = maybeKey match {
      case Some(keyType) =>
        (
          classOf[ContractCompanion.WithKey[_, _, _, _]],
          Seq(toJavaTypeName(keyType)),
          ",$We -> $L",
          Seq(
            FromValueGenerator
              .extractor(keyType, "e", CodeBlock.of("e"), newNameGenerator)
          ),
        )
      case None => (classOf[ContractCompanion.WithoutKey[_, _, _]], Seq.empty, "", Seq.empty)
    }

    private val contractIdName = nestedClassName(templateClassName, "ContractId")
    private val contractName = nestedClassName(templateClassName, "Contract")
    private val companionType = ParameterizedTypeName.get(
      ClassName get fieldClass,
      Seq(
        contractName,
        contractIdName,
        templateClassName,
      ) ++ keyTypes: _*
    )

    private[TemplateClass] def generateGetter(): MethodSpec =
      generateGetCompanion(companionType, ClassGenUtils.companionFieldName)

    private[TemplateClass] def generateField(
        choiceNames: Set[ChoiceName]
    ): FieldSpec = {
      val valueDecoderLambdaArgName = "v"
      FieldSpec
        .builder(
          companionType,
          companionFieldName,
          Modifier.STATIC,
          Modifier.FINAL,
          Modifier.PUBLIC,
        )
        .initializer(
          "$Znew $T<>(new $T($T.$N, $T.$N, $T.$N),$>$Z$S,$W$N,$W$T::new,$W$T::fromJson,$W$T::new,$W$T.of($L),$T.templateValueDecoder()" + keyParams + "$<)",
          Seq[Object](
            fieldClass,
            nestedClassName(ClassName.get(classOf[ContractTypeCompanion[_, _, _, _]]), "Package"),
            templateClassName,
            ClassGenUtils.packageIdFieldName,
            templateClassName,
            ClassGenUtils.packageNameFieldName,
            templateClassName,
            ClassGenUtils.packageVersionFieldName,
            templateClassName,
            templateIdFieldName,
            contractIdName,
            valueDecoderLambdaArgName,
            templateClassName,
            valueDecoderLambdaArgName,
            templateClassName,
            contractName,
            classOf[java.util.List[_]],
            CodeBlock
              .join(
                choiceNames
                  .map(choiceName => CodeBlock.of("$N", toChoiceNameField(choiceName)))
                  .asJava,
                ",$W",
              ),
          ) ++ keyArgs: _*
        )
        .build()
    }
  }

  private implicit final class `MethodSpec extensions`(private val self: MethodSpec.Builder)
      extends AnyVal {
    // for template, use createAnd() or byKey(); toInterface methods need public
    // access if in different packages, though
    private[TemplateClass] def publicIfInterface(
        isInterface: Either[ContractIdClass.For.Interface.type, _]
    ) =
      self.addModifiers(
        isInterface.fold(_ => Some(Modifier.PUBLIC), _ => None).toList.asJava
      )

    private[TemplateClass] def companionIfInterface(
        isInterface: Either[ContractIdClass.For.Interface.type, _]
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
  }

  private implicit final class `TypeSpec extensions`(private val self: TypeSpec.Builder)
      extends AnyVal {
    private[TemplateClass] def addGetCompanion(
        markerName: ClassName,
        isInterface: Either[ContractIdClass.For.Interface.type, _],
    ) =
      self.addMethod(
        ContractIdClass.Builder.generateGetCompanion(
          markerName,
          isInterface.map(_ => ContractIdClass.For.Template).merge,
        )
      )
  }

  def toChoiceNameField(choiceName: ChoiceName): String =
    s"CHOICE_$choiceName"
}
