// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.Value
import com.squareup.javapoet.{ClassName, ParameterSpec, ParameterizedTypeName, TypeVariableName}

private[inner] abstract case class FromValueExtractorParameters(
    typeVariables: IndexedSeq[TypeVariableName],
    parameterSpecs: IndexedSeq[ParameterSpec],
)

private[inner] object FromValueExtractorParameters {

  def generate(typeParameters: IndexedSeq[String]): FromValueExtractorParameters = {
    val typeVars = typeVariableNames(typeParameters)
    new FromValueExtractorParameters(typeVars, typeVars.map(extractorParameter)) {}
  }

  private def typeVariableNames(typeArguments: IndexedSeq[String]): IndexedSeq[TypeVariableName] =
    typeArguments.map(TypeVariableName.get)

  private def extractorParameter(t: TypeVariableName): ParameterSpec =
    ParameterSpec.builder(extractorType(t), s"fromValue$t").build()

  private val function = ClassName.get(classOf[java.util.function.Function[_, _]])
  private val value = ClassName.get(classOf[Value])
  private def extractorType(t: TypeVariableName): ParameterizedTypeName =
    ParameterizedTypeName.get(function, value, t)
}
