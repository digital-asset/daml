// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.squareup.javapoet.MethodSpec
import javax.lang.model.element.Modifier

object ConstructorGenerator {
  def generateConstructor(fields: Fields): MethodSpec = {
    val builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC)
    for (FieldInfo(_, _, javaName, javaType) <- fields) {
      builder.addParameter(javaType, javaName)
      builder.addStatement("this.$L = $L", javaName, javaName)
    }
    builder.build()
  }

}
