// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi
import ClassGenUtils.{companionFieldName, optional, optionalString, setOfStrings}
import com.daml.lf.typesig.Type
import com.squareup.javapoet._
import scalaz.syntax.std.option._

import scala.jdk.CollectionConverters._
import javax.lang.model.element.Modifier

object ContractClass {

  def builder(templateClassName: ClassName, key: Option[Type])(implicit
      packagePrefixes: PackagePrefixes
  ) = Builder.create(templateClassName, key)

  case class Builder private (
      classBuilder: TypeSpec.Builder,
      contractClassName: ClassName,
      templateClassName: ClassName,
      contractIdClassName: ClassName,
      contractKeyClassName: Option[TypeName],
      key: Option[Type],
  )(implicit
      packagePrefixes: PackagePrefixes
  ) {
    def addGenerateFromMethods(): Builder = {
      classBuilder
        .addMethod(
          Builder.generateFromIdAndRecord(
            contractClassName,
            contractKeyClassName,
          )
        )
        .addMethod(
          Builder.generateFromCreatedEvent(contractClassName)
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
        className: ClassName
    ) =
      generateCompanionForwarder(
        "fromCreatedEvent",
        className,
        identity,
        (ClassName get classOf[javaapi.data.CreatedEvent], "event"),
      )

    private[inner] def generateFromIdAndRecord(
        className: ClassName,
        maybeContractKeyClassName: Option[TypeName],
    ): MethodSpec = {
      val methodParameters = Seq(
        (ClassName get classOf[String], "contractId"),
        (ClassName get classOf[javaapi.data.DamlRecord], "record$"),
        (optionalString, agreementFieldName),
      ) ++ maybeContractKeyClassName
        .map(name => (optional(name), contractKeyFieldName))
        .toList ++ Iterable(
        (setOfStrings, signatoriesFieldName),
        (setOfStrings, observersFieldName),
      )

      generateCompanionForwarder("fromIdAndRecord", className, identity, methodParameters: _*)
    }

    private[this] def generateCompanionForwarder(
        methodName: String,
        returns: TypeName,
        otherSettings: MethodSpec.Builder => MethodSpec.Builder,
        parameters: (TypeName, String)*
    ): MethodSpec = {
      val methodParameters = parameters.map { case (t, n) => ParameterSpec.builder(t, n).build() }
      val spec = MethodSpec
        .methodBuilder(methodName)
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(returns)
        .addParameters(methodParameters.asJava)
        .addStatement(
          "return $N.$N($L)",
          companionFieldName,
          methodName,
          CodeBlock
            .join(parameters.map { case (_, pName) => CodeBlock.of("$N", pName) }.asJava, ",$W"),
        )
      otherSettings(spec).build()
    }

    private[this] val contractIdClassName = ClassName bestGuess "ContractId"
    private[this] val contractClassName = ClassName bestGuess "Contract"

    private[this] def generateGetCompanion(templateClassName: ClassName): MethodSpec = {
      ClassGenUtils.generateGetCompanion(
        ParameterizedTypeName.get(
          ClassName get classOf[javaapi.data.codegen.ContractCompanion[_, _, _]],
          contractClassName,
          contractIdClassName,
          templateClassName,
        ),
        companionFieldName,
      )
    }

    def create(templateClassName: ClassName, key: Option[Type])(implicit
        packagePrefixes: PackagePrefixes
    ) = {
      val classBuilder =
        TypeSpec.classBuilder("Contract").addModifiers(Modifier.STATIC, Modifier.PUBLIC)
      val contractKeyClassName = key.map(toJavaTypeName(_))

      import scala.language.existentials
      val (contractSuperclass, keyTparams) = contractKeyClassName.cata(
        kname => (classOf[javaapi.data.codegen.ContractWithKey[_, _, _]], Seq(kname)),
        (classOf[javaapi.data.codegen.Contract[_, _]], Seq.empty),
      )
      classBuilder.superclass(
        ParameterizedTypeName.get(
          ClassName get contractSuperclass,
          Seq(contractIdClassName, templateClassName) ++ keyTparams: _*
        )
      )

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

      val superCtorKeyArgs = contractKeyClassName.map(_ => contractKeyFieldName).toList
      constructorBuilder.addStatement(
        "super($L)",
        CodeBlock.join(
          (Seq(idFieldName, dataFieldName, agreementFieldName)
            ++ superCtorKeyArgs
            ++ Seq(signatoriesFieldName, observersFieldName)).map(CodeBlock.of("$L", _)).asJava,
          ",$W",
        ),
      )

      val constructor = constructorBuilder.build()

      classBuilder.addMethod(constructor)

      classBuilder
        .addMethod(generateGetCompanion(templateClassName))
      new Builder(
        classBuilder,
        contractClassName,
        templateClassName,
        contractIdClassName,
        contractKeyClassName,
        key,
      )
    }
  }
}
