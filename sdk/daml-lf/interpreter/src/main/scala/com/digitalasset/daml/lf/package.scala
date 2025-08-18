// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import scala.collection.immutable.ArraySeq

package object speedy {

  val Compiler = compiler.Compiler
  type Compiler = compiler.Compiler

  private[speedy] def buildArraySeq[X <: AnyRef](size: Int)(
      init: Array[AnyRef] => Unit
  ): ArraySeq[X] = {
    val array = new Array[AnyRef](size)
    init(array)
    ArraySeq.unsafeWrapArray(array).asInstanceOf[ArraySeq[X]]
  }

}
