// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import com.squareup.javapoet.FieldSpec
import javax.lang.model.element.Modifier

object RecordFields {

  def apply(fields: Fields): IndexedSeq[FieldSpec] =
    fields.map { case FieldInfo(_, _, javaName, javaType) =>
      FieldSpec
        .builder(javaType, javaName)
        .addModifiers(Modifier.FINAL, Modifier.PUBLIC)
        .build()
    }

}
