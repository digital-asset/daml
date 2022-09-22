// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.Value
import com.daml.ledger.javaapi.data.codegen.ValueDecoder
import com.squareup.javapoet.{ClassName, ParameterSpec, ParameterizedTypeName, TypeVariableName}

private[inner] abstract case class FromValueExtractorParameters(
    typeVariables: IndexedSeq[TypeVariableName],
    functionParameterSpecs: IndexedSeq[ParameterSpec],
    fromValueParameterSpecs: IndexedSeq[ParameterSpec],
)

private[inner] object FromValueExtractorParameters {

  def generate(typeParameters: IndexedSeq[String]): FromValueExtractorParameters = {
    val typeVars = typeVariableNames(typeParameters)
    new FromValueExtractorParameters(
      typeVars,
      typeVars.map(extractorFunctionParameter),
      typeVars.map(extractorFromValueParameter),
    ) {}
  }

  private def typeVariableNames(typeArguments: IndexedSeq[String]): IndexedSeq[TypeVariableName] =
    typeArguments.map(TypeVariableName.get)

  private def extractorFunctionParameter(t: TypeVariableName): ParameterSpec =
    ParameterSpec.builder(extractorFunctionType(t), s"fromValue$t").build()

  private def extractorFromValueParameter(t: TypeVariableName): ParameterSpec =
    ParameterSpec.builder(extractorFromValueType(t), s"fromValue$t").build()

  private val fromValue = ClassName.get(classOf[ValueDecoder[_]])
  private val function = ClassName.get(classOf[java.util.function.Function[_, _]])
  private val value = ClassName.get(classOf[Value])

  private def extractorFunctionType(t: TypeVariableName): ParameterizedTypeName =
    ParameterizedTypeName.get(function, value, t)

  private def extractorFromValueType(t: TypeVariableName): ParameterizedTypeName =
    ParameterizedTypeName.get(fromValue, t)
}
