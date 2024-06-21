// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.digitalasset.daml.lf.codegen.NodeWithContext.AuxiliarySignatures
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.ChoiceName
import com.digitalasset.daml.lf.typesig
import typesig.{TemplateChoice, Type}
import com.squareup.javapoet._
import ClassGenUtils.companionFieldName
import com.daml.ledger.javaapi.data.codegen.{Exercised, Update}

import javax.lang.model.element.Modifier
import scala.jdk.CollectionConverters._

object ContractIdClass {

  sealed abstract class For extends Product with Serializable
  object For {
    case object Interface extends For
    case object Template extends For
  }

  def builder(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[typesig.Type]],
      kind: For,
  ) = Builder.create(
    templateClassName,
    choices,
    kind,
  )

  case class Builder private (
      templateClassName: ClassName,
      contractIdClassName: ClassName,
      idClassBuilder: TypeSpec.Builder,
      choices: Map[ChoiceName, TemplateChoice[typesig.Type]],
  ) {
    def build(): TypeSpec = idClassBuilder.build()

    def addConversionForImplementedInterfaces(implementedInterfaces: Seq[Ref.TypeConName])(implicit
        packagePrefixes: PackagePrefixes
    ): Builder = {
      idClassBuilder.addMethods(
        generateToInterfaceMethods(
          "ContractId",
          "this.contractId",
          implementedInterfaces,
        ).asJava
      )
      implementedInterfaces.foreach { interfaceName =>
        val name =
          ClassName.bestGuess(fullyQualifiedName(interfaceName))
        val interfaceContractIdName = nestedClassName(name, "ContractId")
        val tplContractIdClassName = nestedClassName(templateClassName, "ContractId")
        idClassBuilder.addMethod(
          MethodSpec
            .methodBuilder(s"unsafeFromInterface")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(interfaceContractIdName, "interfaceContractId")
            .addStatement(
              "return new ContractId(interfaceContractId.contractId)"
            )
            .returns(tplContractIdClassName)
            .build()
        )
      }
      this
    }

    def addContractIdConversionCompanionForwarder(): Builder = {
      idClassBuilder
        .addMethod(
          Builder.fromContractId(
            contractIdClassName,
            templateClassName,
          )
        )
      this
    }
  }

  private val updateType: ClassName = ClassName get classOf[Update[_]]
  private val exercisedType: ClassName = ClassName get classOf[Exercised[_]]
  private def parameterizedUpdateType(returnTypeName: TypeName): TypeName =
    ParameterizedTypeName.get(updateType, ParameterizedTypeName.get(exercisedType, returnTypeName))
  private val exercisesTypeParam = TypeVariableName get "Cmd"
  private[inner] def exercisesInterface(className: ClassName) =
    nestedClassName(className, "Exercises")

  private[inner] def generateExercisesInterface(
      className: ClassName,
      choices: Map[ChoiceName, TemplateChoice.FWT],
      typeDeclarations: AuxiliarySignatures,
  )(implicit
      packagePrefixes: PackagePrefixes
  ) = {
    val exercisesParent =
      if (hasStandardArchive(choices, typeDeclarations))
        classOf[javaapi.data.codegen.Exercises.Archivable[_]]
      else classOf[javaapi.data.codegen.Exercises[_]]
    val exercisesClass = TypeSpec
      .interfaceBuilder(exercisesInterface(className))
      .addTypeVariable(exercisesTypeParam)
      .addSuperinterface(
        ParameterizedTypeName
          .get(ClassName get exercisesParent, exercisesTypeParam)
      )
      .addModifiers(Modifier.PUBLIC)
    choices foreach { case (choiceName, choice) =>
      exercisesClass addMethod Builder.generateExerciseMethod(
        choiceName,
        choice,
      )
      for (
        record <- choice.param.fold(
          ClassGenUtils.getRecord(_, typeDeclarations),
          _ => None,
          _ => None,
          _ => None,
        )
      ) {
        val splatted = Builder.generateFlattenedExerciseMethod(
          choiceName,
          choice,
          getFieldsWithTypes(record.fields),
        )
        exercisesClass.addMethod(splatted)
      }
    }
    exercisesClass.build()
  }

  private[this] def hasStandardArchive(
      choices: Map[ChoiceName, TemplateChoice.FWT],
      typeDeclarations: AuxiliarySignatures,
  ) =
    choices get ClassGenUtils.archiveChoiceName exists {
      case typesig.TemplateChoice(
            tc: typesig.TypeCon,
            true,
            typesig.TypePrim(typesig.PrimType.Unit, Seq()),
          ) =>
        ClassGenUtils.getRecord(tc, typeDeclarations) exists { rec =>
          rec.fields.isEmpty
        }
      case _ => false
    }

  private[inner] def generateToInterfaceMethods(
      nestedReturn: String,
      selfArgs: String,
      implementedInterfaces: Seq[Ref.TypeConName],
  )(implicit
      packagePrefixes: PackagePrefixes
  ) =
    implementedInterfaces.map { interfaceName =>
      val name = ClassName.bestGuess(fullyQualifiedName(interfaceName))
      val interfaceContractIdName = nestedClassName(name, nestedReturn)
      MethodSpec
        .methodBuilder("toInterface")
        .addModifiers(Modifier.PUBLIC)
        .addParameter(name nestedClass InterfaceClass.companionClassName, "interfaceCompanion")
        .addStatement("return new $T($L)", interfaceContractIdName, selfArgs)
        .returns(interfaceContractIdName)
        .build()
    }

  private[inner] object Builder {

    private[ContractIdClass] def generateFlattenedExerciseMethod(
        choiceName: ChoiceName,
        choice: TemplateChoice[Type],
        fields: Fields,
    )(implicit
        packagePrefixes: PackagePrefixes
    ): MethodSpec =
      ClassGenUtils.generateFlattenedCreateOrExerciseMethod(
        "exercise",
        parameterizedUpdateType(toJavaTypeName(choice.returnType)),
        choiceName,
        choice,
        fields,
      )(_.addModifiers(Modifier.DEFAULT))

    private[ContractIdClass] def generateExerciseMethod(
        choiceName: ChoiceName,
        choice: TemplateChoice[Type],
    )(implicit
        packagePrefixes: PackagePrefixes
    ): MethodSpec = {
      val methodName = s"exercise${choiceName.capitalize}"
      val exerciseChoiceBuilder = MethodSpec
        .methodBuilder(methodName)
        .addModifiers(Modifier.DEFAULT, Modifier.PUBLIC)
        .returns(parameterizedUpdateType(toJavaTypeName(choice.returnType)))
      val javaType = toJavaTypeName(choice.param)
      exerciseChoiceBuilder.addParameter(javaType, "arg")
      exerciseChoiceBuilder.addStatement(
        "return makeExerciseCmd($L, arg)",
        TemplateClass.toChoiceNameField(choiceName),
      )
      exerciseChoiceBuilder.build()
    }

    def generateGetCompanion(markerName: ClassName, kind: For) = {
      val contractIdClassName = nestedClassName(markerName, "ContractId")
      ClassGenUtils.generateGetCompanion(
        ParameterizedTypeName.get(
          ClassName get classOf[javaapi.data.codegen.ContractTypeCompanion[_, _, _, _]],
          WildcardTypeName subtypeOf ParameterizedTypeName.get(
            ClassName get classOf[javaapi.data.codegen.Contract[_, _]],
            contractIdClassName,
            WildcardTypeName subtypeOf classOf[Object],
          ),
          contractIdClassName,
          markerName,
          WildcardTypeName subtypeOf classOf[Object],
        ),
        kind match {
          case For.Interface => InterfaceClass.companionName
          case For.Template => ClassGenUtils.companionFieldName
        },
      )
    }

    private[inner] def fromContractId(
        className: ClassName,
        templateClassName: ClassName,
    ): MethodSpec = {
      val spec = MethodSpec
        .methodBuilder("fromContractId")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(className)
        .addParameters(
          Seq(
            ParameterSpec
              .builder(
                ParameterizedTypeName
                  .get(
                    ClassName.get(classOf[javaapi.data.codegen.ContractId[_]]),
                    templateClassName,
                  ),
                "contractId",
              )
              .build()
          ).asJava
        )
        .addStatement(
          "return $N.$N($L)",
          companionFieldName,
          "toContractId",
          "contractId",
        )
      spec.build()
    }

    def create(
        templateClassName: ClassName,
        choices: Map[ChoiceName, TemplateChoice[typesig.Type]],
        kind: For,
    ): Builder = {
      val contractIdClassName = nestedClassName(templateClassName, "ContractId")
      val idClassBuilder =
        TypeSpec
          .classBuilder(contractIdClassName.simpleName)
          .superclass(
            ParameterizedTypeName
              .get(ClassName.get(classOf[javaapi.data.codegen.ContractId[_]]), templateClassName)
          )
          .addSuperinterface(
            ParameterizedTypeName
              .get(
                exercisesInterface(templateClassName),
                ClassName get classOf[javaapi.data.ExerciseCommand],
              )
          )
          .addModifiers(Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC)
      val constructor =
        MethodSpec
          .constructorBuilder()
          .addModifiers(Modifier.PUBLIC)
          .addParameter(ClassName.get(classOf[String]), "contractId")
          .addStatement("super(contractId)")
          .build()
      idClassBuilder
        .addMethod(constructor)
        .addMethod(generateGetCompanion(templateClassName, kind))
      Builder(templateClassName, contractIdClassName, idClassBuilder, choices)
    }
  }
}
