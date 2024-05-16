// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data._
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.javaapi.data.codegen.ContractDecoder
import com.squareup.javapoet._

import java.util
import javax.lang.model.element.Modifier

import scala.jdk.CollectionConverters._

object DecoderClass {

  // Generates the Decoder class to lookup template companion for known templates
  // from Record => $TemplateClass
  def generateCode(simpleClassName: String, templateNames: Iterable[ClassName]): TypeSpec = {
    TypeSpec
      .classBuilder(simpleClassName)
      .addModifiers(Modifier.PUBLIC)
      .addField(decodersField)
      .addMethod(fromCreatedEvent)
      .addMethod(getDecoder)
      .addMethod(getJsonDecoder)
      .addStaticBlock(generateStaticInitializer(templateNames))
      .build()
  }

  private val contractType = ClassName.get(
    classOf[Contract]
  )

  private val decoderFunctionType = ParameterizedTypeName.get(
    ClassName.get(classOf[java.util.function.Function[_, _]]),
    ClassName.get(classOf[CreatedEvent]),
    ClassName.get(classOf[Contract]),
  )

  private val contractDecoderType = ClassName.get(classOf[ContractDecoder])

  private val fromCreatedEvent = MethodSpec
    .methodBuilder("fromCreatedEvent")
    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
    .returns(contractType)
    .addParameter(ClassName.get(classOf[CreatedEvent]), "event")
    .addException(classOf[IllegalArgumentException])
    .addStatement("return contractDecoder.fromCreatedEvent(event)")
    .build()

  private val getDecoder = MethodSpec
    .methodBuilder("getDecoder")
    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
    .returns(
      ParameterizedTypeName.get(ClassName.get(classOf[java.util.Optional[_]]), decoderFunctionType)
    )
    .addParameter(ClassName.get(classOf[Identifier]), "templateId")
    .addStatement("return contractDecoder.getDecoder(templateId)")
    .build()

  private val getJsonDecoder = {
    // Optional<ContractCompanion.FromJson<? extends DamlRecord<?>>>
    val returnType = ParameterizedTypeName.get(
      ClassName.get(classOf[java.util.Optional[_]]),
      ParameterizedTypeName.get(
        ClassName.get(classOf[ContractCompanion.FromJson[_]]),
        WildcardTypeName.subtypeOf(
          ParameterizedTypeName.get(
            ClassName.get(classOf[com.daml.ledger.javaapi.data.codegen.DamlRecord[_]]),
            WildcardTypeName.subtypeOf(classOf[Object]),
          )
        ),
      ),
    )
    MethodSpec
      .methodBuilder("getJsonDecoder")
      .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
      .returns(returnType)
      .addParameter(ClassName.get(classOf[Identifier]), "templateId")
      .addStatement("return contractDecoder.getJsonDecoder(templateId)")
      .build()
  }

  private val decodersField = FieldSpec
    .builder(contractDecoderType, "contractDecoder")
    .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
    .build()

  def generateStaticInitializer(templateNames: Iterable[ClassName]): CodeBlock = {
    CodeBlock
      .builder()
      .addStatement(
        "$N = new $T($T.asList($L))".stripMargin,
        "contractDecoder",
        contractDecoderType,
        ClassName.get(classOf[util.Arrays]),
        CodeBlock.join(
          templateNames.map { template =>
            CodeBlock.of(
              "$T.COMPANION",
              template,
            )
          }.asJava,
          ",$W",
        ),
      )
      .build()
  }
}
