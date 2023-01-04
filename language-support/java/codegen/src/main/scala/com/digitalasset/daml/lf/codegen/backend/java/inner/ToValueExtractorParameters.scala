// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.daml.ledger.javaapi.data.Value
import com.squareup.javapoet.{ClassName, ParameterSpec, ParameterizedTypeName, TypeVariableName}

private[inner] object ToValueExtractorParameters {

  def generate(typeParameters: IndexedSeq[String]): IndexedSeq[ParameterSpec] =
    typeParameters.map(extractorParameter)

  private def extractorParameter(t: String): ParameterSpec =
    ParameterSpec.builder(extractorType(TypeVariableName.get(t)), s"toValue$t").build()

  private val function = ClassName.get(classOf[java.util.function.Function[_, _]])
  private val value = ClassName.get(classOf[Value])
  private def extractorType(t: TypeVariableName): ParameterizedTypeName =
    ParameterizedTypeName.get(function, t, value)
}
