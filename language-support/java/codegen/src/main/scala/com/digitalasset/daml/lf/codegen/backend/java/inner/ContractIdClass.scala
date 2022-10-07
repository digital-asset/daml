// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.typesig
import typesig.{TemplateChoice, Type}
import typesig.PackageSignature.TypeDecl
import com.squareup.javapoet._
import ClassGenUtils.companionFieldName
import com.daml.ledger.javaapi.data.codegen.Update

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
      packagePrefixes: Map[PackageId, String],
  ) = Builder.create(
    templateClassName,
    choices,
    kind,
    packagePrefixes,
  )

  case class Builder private (
      templateClassName: ClassName,
      contractIdClassName: ClassName,
      idClassBuilder: TypeSpec.Builder,
      choices: Map[ChoiceName, TemplateChoice[typesig.Type]],
      packagePrefixes: Map[PackageId, String],
  ) {
    def build(): TypeSpec = idClassBuilder.build()

    def addConversionForImplementedInterfaces(
        implementedInterfaces: Seq[Ref.TypeConName]
    ): Builder = {
      idClassBuilder.addMethods(
        generateToInterfaceMethods("ContractId", "this.contractId", implementedInterfaces).asJava
      )
      implementedInterfaces.foreach { interfaceName =>
        // XXX why doesn't this use packagePrefixes? -SC
        val name = ClassName.bestGuess(fullyQualifiedName(interfaceName.qualifiedName))
        val interfaceContractIdName = name nestedClass "ContractId"
        val tplContractIdClassName = templateClassName.nestedClass("ContractId")
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
  private def parameterizedUpdateType(returnTypeName: TypeName): TypeName =
    ParameterizedTypeName.get(updateType, returnTypeName)
  private val exercisesTypeParam = TypeVariableName get "Cmd"
  private[inner] val exercisesInterface = ClassName bestGuess "Exercises"

  private[inner] def generateExercisesInterface(
      choices: Map[ChoiceName, TemplateChoice.FWT],
      typeDeclarations: Map[QualifiedName, TypeDecl],
      packageId: PackageId,
      packagePrefixes: Map[PackageId, String],
  ) = {
    val exercisesClass = TypeSpec
      .interfaceBuilder(exercisesInterface)
      .addTypeVariable(exercisesTypeParam)
      .addSuperinterface(
        ParameterizedTypeName
          .get(ClassName get classOf[javaapi.data.codegen.Exercises[_]], exercisesTypeParam)
      )
      .addModifiers(Modifier.PUBLIC)
    choices foreach { case (choiceName, choice) =>
      exercisesClass addMethod Builder.generateExerciseMethod(
        choiceName,
        choice,
        packagePrefixes,
      )
      for (
        record <- choice.param.fold(
          ClassGenUtils.getRecord(_, typeDeclarations, packageId),
          _ => None,
          _ => None,
          _ => None,
        )
      ) {
        val splatted = Builder.generateFlattenedExerciseMethod(
          choiceName,
          choice,
          getFieldsWithTypes(record.fields, packagePrefixes),
          packagePrefixes,
        )
        exercisesClass.addMethod(splatted)
      }
    }
    exercisesClass.build()
  }

  private[inner] def generateToInterfaceMethods(
      nestedReturn: String,
      selfArgs: String,
      implementedInterfaces: Seq[Ref.TypeConName],
  ) =
    implementedInterfaces.map { interfaceName =>
      // XXX why doesn't this use packagePrefixes? -SC
      val name = ClassName.bestGuess(fullyQualifiedName(interfaceName.qualifiedName))
      val interfaceContractIdName = name nestedClass nestedReturn
      MethodSpec
        .methodBuilder("toInterface")
        .addModifiers(Modifier.PUBLIC)
        .addParameter(name nestedClass InterfaceClass.companionName, "interfaceCompanion")
        .addStatement("return new $T($L)", interfaceContractIdName, selfArgs)
        .returns(interfaceContractIdName)
        .build()
    }

  private[inner] object Builder {

    private[ContractIdClass] def generateFlattenedExerciseMethod(
        choiceName: ChoiceName,
        choice: TemplateChoice[Type],
        fields: Fields,
        packagePrefixes: Map[PackageId, String],
    ): MethodSpec =
      ClassGenUtils.generateFlattenedCreateOrExerciseMethod(
        "exercise",
        parameterizedUpdateType(toJavaTypeName(choice.returnType, packagePrefixes)),
        choiceName,
        choice,
        fields,
        packagePrefixes,
      )(_.addModifiers(Modifier.DEFAULT))

    private[ContractIdClass] def generateExerciseMethod(
        choiceName: ChoiceName,
        choice: TemplateChoice[Type],
        packagePrefixes: Map[PackageId, String],
    ): MethodSpec = {
      val methodName = s"exercise${choiceName.capitalize}"
      val exerciseChoiceBuilder = MethodSpec
        .methodBuilder(methodName)
        .addModifiers(Modifier.DEFAULT, Modifier.PUBLIC)
        .returns(parameterizedUpdateType(toJavaTypeName(choice.returnType, packagePrefixes)))
      val javaType = toJavaTypeName(choice.param, packagePrefixes)
      exerciseChoiceBuilder.addParameter(javaType, "arg")
//      choice.param match {
//        case TypeCon(_, _) =>
//          exerciseChoiceBuilder.addStatement(
//            "$T argValue = arg.toValue()",
//            classOf[javaapi.data.Value],
//          )
//        case TypePrim(PrimType.Unit, ImmArraySeq()) =>
//          exerciseChoiceBuilder
//            .addStatement(
//              "$T argValue = $T.getInstance()",
//              classOf[javaapi.data.Value],
//              classOf[javaapi.data.Unit],
//            )
//        case TypePrim(_, _) | TypeVar(_) | TypeNumeric(_) =>
//          exerciseChoiceBuilder
//            .addStatement(
//              "$T argValue = new $T(arg)",
//              classOf[javaapi.data.Value],
//              toAPITypeName(choice.param),
//            )
//      }
      exerciseChoiceBuilder.addStatement(
        "return makeExerciseCmd($L, arg)",
        TemplateClass.toChoiceNameField(choiceName),
      )
      exerciseChoiceBuilder.build()
    }

    def generateGetCompanion(markerName: ClassName, kind: For) =
      ClassGenUtils.generateGetCompanion(
        ParameterizedTypeName.get(
          ClassName get classOf[javaapi.data.codegen.ContractTypeCompanion[_, _]],
          markerName,
          WildcardTypeName subtypeOf classOf[Object],
        ),
        kind match {
          case For.Interface => InterfaceClass.companionName
          case For.Template => ClassGenUtils.companionFieldName
        },
      )

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
        packagePrefixes: Map[PackageId, String],
    ): Builder = {

      val contractIdClassName = ClassName bestGuess "ContractId"
      val idClassBuilder =
        TypeSpec
          .classBuilder("ContractId")
          .superclass(
            ParameterizedTypeName
              .get(ClassName.get(classOf[javaapi.data.codegen.ContractId[_]]), templateClassName)
          )
          .addSuperinterface(
            ParameterizedTypeName
              .get(exercisesInterface, ClassName get classOf[javaapi.data.ExerciseCommand])
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
      Builder(templateClassName, contractIdClassName, idClassBuilder, choices, packagePrefixes)
    }
  }
}
