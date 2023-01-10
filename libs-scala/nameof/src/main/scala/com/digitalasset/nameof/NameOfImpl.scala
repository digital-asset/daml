// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nameof

import scala.reflect.macros.whitebox

object NameOfImpl {
  def nameOf(c: whitebox.Context): c.Expr[String] = {
    import c.universe._
    val cc = c.asInstanceOf[scala.reflect.macros.runtime.Context]
    c.Expr[String](q"${cc.callsiteTyper.context.enclMethod.owner.fullName}")
  }
}
