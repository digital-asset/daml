// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import com.daml.lf.codegen.backend.java.ObjectMethods
import com.daml.lf.codegen.backend.java.inner.ClassGenUtils.{
  emptyOptional,
  emptySet,
  getAgreementText,
  getArguments,
  getContractId,
  getContractKey,
  getObservers,
  getSignatories,
  optional,
  optionalString,
  setOfStrings,
}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.iface.Type
import com.squareup.javapoet._

import scala.jdk.CollectionConverters._
import javax.lang.model.element.Modifier

object ContractIdClass {

  def builder(
      templateClassName: ClassName,
      key: Option[Type],
      packagePrefixes: Map[PackageId, String],
  ) = Builder.create(templateClassName, key, packagePrefixes)

  case class Builder private (
      classBuilder: TypeSpec.Builder,
      contractClassName: ClassName,
      templateClassName: ClassName,
      contractIdClassName: ClassName,
      contractKeyClassName: Option[TypeName],
      key: Option[Type],
      packagePrefixes: Map[PackageId, String],
  ) {
    def addGenerateFromMethods(): Builder = {
      classBuilder
        .addMethod(
          Builder.generateFromIdAndRecord(
            contractClassName,
            templateClassName,
            contractIdClassName,
            contractKeyClassName,
          )
        )
        .addMethod(
          Builder.generateFromIdAndRecordDeprecated(
            contractClassName,
            templateClassName,
            contractIdClassName,
            contractKeyClassName,
          )
        )
        .addMethod(
          Builder.generateFromCreatedEvent(
            contractClassName,
            key,
            packagePrefixes,
          )
        )
      this
    }

    def build() = classBuilder.build()
  }

  object Builder {
    private val idFieldName = "id"
    private val dataFieldName = "data"
    private val agreementFieldName = "agreementText"
    private val contractKeyFieldName = "key"
    private val signatoriesFieldName = "signatories"
    private val observersFieldName = "observers"

    private def generateFromCreatedEvent(
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

    private def generateFromIdAndRecordDeprecated(
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

    def create(
        templateClassName: ClassName,
        key: Option[Type],
        packagePrefixes: Map[PackageId, String],
    ) = {
      val classBuilder =
        TypeSpec.classBuilder("Contract").addModifiers(Modifier.STATIC, Modifier.PUBLIC)
      val contractIdClassName = ClassName.bestGuess("ContractId")
      val contractKeyClassName = key.map(toJavaTypeName(_, packagePrefixes))

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
        .addMethods(
          ObjectMethods(contractClassName, IndexedSeq.empty, fields, templateClassName).asJava
        )
      new Builder(
        classBuilder,
        contractClassName,
        templateClassName,
        contractIdClassName,
        contractKeyClassName,
        key,
        packagePrefixes,
      )
    }
  }
}
