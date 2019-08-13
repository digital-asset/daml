// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend
import com.squareup.javapoet.AnnotationSpec

package object java {

  object Annotation {
    val `override` = AnnotationSpec.builder(classOf[Override]).build()
  }

}
