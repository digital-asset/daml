// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.backend

import com.squareup.javapoet.AnnotationSpec

package object java {

  object Annotation {
    val `override` = AnnotationSpec.builder(classOf[Override]).build()
  }

}
