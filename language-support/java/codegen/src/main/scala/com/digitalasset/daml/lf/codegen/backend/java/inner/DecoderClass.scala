// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data._
import com.squareup.javapoet._
import javax.lang.model.element.Modifier

object DecoderClass {

  // Generates the Decoder class to lookup template decoders for known templates
  // from Record => $TemplateClass
  def generateCode(simpleClassName: String, templateNames: Iterable[ClassName]): TypeSpec = {
    TypeSpec
      .classBuilder(simpleClassName)
      .addModifiers(Modifier.PUBLIC)
      .addField(decodersField)
      .addMethod(fromCreatedEvent)
      .addMethod(getDecoder)
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

  private val decodersMapType = ParameterizedTypeName.get(
    ClassName.get(classOf[java.util.HashMap[_, _]]),
    ClassName.get(classOf[Identifier]),
    decoderFunctionType,
  )

  private val fromCreatedEvent = MethodSpec
    .methodBuilder("fromCreatedEvent")
    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
    .returns(contractType)
    .addParameter(ClassName.get(classOf[CreatedEvent]), "event")
    .addException(classOf[IllegalArgumentException])
    .addCode(
      CodeBlock
        .builder()
        .addStatement("Identifier templateId = event.getTemplateId()")
        .addStatement(
          "$T decoderFunc = getDecoder(templateId).orElseThrow(() -> new IllegalArgumentException(\"No template found for identifier \" + templateId))",
          decoderFunctionType,
        )
        .addStatement("return decoderFunc.apply(event)")
        .build()
    )
    .build()

  private val getDecoder = MethodSpec
    .methodBuilder("getDecoder")
    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
    .returns(
      ParameterizedTypeName.get(ClassName.get(classOf[java.util.Optional[_]]), decoderFunctionType)
    )
    .addParameter(ClassName.get(classOf[Identifier]), "templateId")
    .addStatement(CodeBlock.of("return Optional.ofNullable(decoders.get(templateId))"))
    .build()

  private val decodersField = FieldSpec
    .builder(decodersMapType, "decoders")
    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
    .build()

  def generateStaticInitializer(templateNames: Iterable[ClassName]) = {
    val b = CodeBlock.builder()
    b.addStatement("$N = new $T()", decodersField, decodersMapType)
    templateNames.foreach { template =>
      b.addStatement(
        "$N.put($T.TEMPLATE_ID, $T.Contract::fromCreatedEvent)",
        decodersField,
        template,
        template,
      )
    }
    b.build()
  }
}
