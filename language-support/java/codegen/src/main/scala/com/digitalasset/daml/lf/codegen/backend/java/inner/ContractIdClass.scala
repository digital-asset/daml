// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{ChoiceName, PackageId, QualifiedName}
import com.daml.lf.iface.{
  InterfaceType,
  PrimType,
  TemplateChoice,
  Type,
  TypeCon,
  TypeNumeric,
  TypePrim,
  TypeVar,
}
import com.squareup.javapoet._

import javax.lang.model.element.Modifier

object ContractIdClass {

  def builder(
      templateClassName: ClassName,
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      packagePrefixes: Map[PackageId, String],
  ) = Builder.create(
    templateClassName,
    choices,
    packagePrefixes,
  )

  case class Builder private (
      templateClassName: ClassName,
      idClassBuilder: TypeSpec.Builder,
      choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
      packagePrefixes: Map[PackageId, String],
  ) {
    def build(): TypeSpec = idClassBuilder.build()

    def addConversionForImplementedInterfaces(
        implementedInterfaces: Seq[Ref.TypeConName]
    ): Builder = {
      implementedInterfaces.foreach { interfaceName =>
        val name = ClassName.bestGuess(fullyQualifiedName(interfaceName.qualifiedName))
        val simpleName = interfaceName.qualifiedName.name.segments.last
        idClassBuilder.addMethod(
          MethodSpec
            .methodBuilder(s"to$simpleName")
            .addModifiers(Modifier.PUBLIC)
            .addStatement(s"return new $name.ContractId(this.contractId)")
            .returns(ClassName.bestGuess(s"$name.ContractId"))
            .build()
        )
        val tplContractIdClassName = templateClassName.nestedClass("ContractId")
        idClassBuilder.addMethod(
          MethodSpec
            .methodBuilder(s"unsafeFrom$simpleName")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .addParameter(ClassName.bestGuess(s"$name.ContractId"), "interfaceContractId")
            .addStatement(
              s"return new ContractId(interfaceContractId.contractId)"
            )
            .returns(tplContractIdClassName)
            .build()
        )
      }
      this
    }

    def addFlattenedExerciseMethods(
        typeDeclarations: Map[QualifiedName, InterfaceType],
        packageId: PackageId,
    ): Builder = {
      for ((choiceName, choice) <- choices) {
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
          idClassBuilder.addMethod(splatted)
        }
      }
      this
    }
  }

  private[inner] object Builder {

    private def generateFlattenedExerciseMethod(
        choiceName: ChoiceName,
        choice: TemplateChoice[Type],
        fields: Fields,
        packagePrefixes: Map[PackageId, String],
    ): MethodSpec =
      ClassGenUtils.generateFlattenedCreateOrExerciseMethod[javaapi.data.ExerciseCommand](
        "exercise",
        choiceName,
        choice,
        fields,
        packagePrefixes,
      )

    private[inner] def generateExerciseMethod(
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

    def create(
        templateClassName: ClassName,
        choices: Map[ChoiceName, TemplateChoice[com.daml.lf.iface.Type]],
        packagePrefixes: Map[PackageId, String],
    ): Builder = {

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
      Builder(templateClassName, idClassBuilder, choices, packagePrefixes)
    }
  }
}
