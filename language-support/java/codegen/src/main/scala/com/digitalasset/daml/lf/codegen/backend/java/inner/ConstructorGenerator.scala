// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend.java.inner

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
