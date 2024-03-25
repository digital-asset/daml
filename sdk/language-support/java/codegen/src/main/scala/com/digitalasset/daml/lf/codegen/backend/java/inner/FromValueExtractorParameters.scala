// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.Value
import com.daml.ledger.javaapi.data.codegen.ValueDecoder
import com.squareup.javapoet.{ClassName, ParameterSpec, ParameterizedTypeName, TypeVariableName}

private[inner] abstract case class FromValueExtractorParameters(
    typeVariables: IndexedSeq[TypeVariableName],
    functionParameterSpecs: IndexedSeq[ParameterSpec],
    valueDecoderParameterSpecs: IndexedSeq[ParameterSpec],
)

private[inner] object FromValueExtractorParameters {

  def generate(typeParameters: IndexedSeq[String]): FromValueExtractorParameters = {
    val typeVars = typeVariableNames(typeParameters)
    new FromValueExtractorParameters(
      typeVars,
      typeVars.map(extractorFunctionParameter),
      typeVars.map(extractorValueDecoderParameter),
    ) {}
  }

  private def typeVariableNames(typeArguments: IndexedSeq[String]): IndexedSeq[TypeVariableName] =
    typeArguments.map(TypeVariableName.get)

  private def extractorFunctionParameter(t: TypeVariableName): ParameterSpec =
    ParameterSpec.builder(extractorFunctionType(t), s"fromValue$t").build()

  private def extractorValueDecoderParameter(t: TypeVariableName): ParameterSpec =
    ParameterSpec.builder(extractorValueDecoderType(t), s"fromValue$t").build()

  private val valueDecoder = ClassName.get(classOf[ValueDecoder[_]])
  private val function = ClassName.get(classOf[java.util.function.Function[_, _]])
  private val value = ClassName.get(classOf[Value])

  private def extractorFunctionType(t: TypeVariableName): ParameterizedTypeName =
    ParameterizedTypeName.get(function, value, t)

  private def extractorValueDecoderType(t: TypeVariableName): ParameterizedTypeName =
    ParameterizedTypeName.get(valueDecoder, t)
}
