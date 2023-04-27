// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nameof

import scala.annotation.tailrec
import scala.reflect.macros.whitebox

object NameOfImpl {
  def nameOfCurrentFunc(c: whitebox.Context): c.Expr[String] = {
    import c.universe._
    val cc = c.asInstanceOf[scala.reflect.macros.runtime.Context]
    c.Expr[String](q"${cc.callsiteTyper.context.enclMethod.owner.fullName}")
  }

  def nameOfMember[A](c: whitebox.Context)(func: c.Expr[A => Any]): c.Expr[String] = {
    import c.universe._

    @tailrec def stripFunctionApply(tree: Tree): Tree = {
      tree match {
        case Function(_, body) => stripFunctionApply(body)
        case Apply(target, _) => stripFunctionApply(target)
        case TypeApply(target, _) => stripFunctionApply(target)
        case _ => tree
      }
    }
    val symbol = stripFunctionApply(func.tree).symbol.fullName
    c.Expr[String](q"""$symbol""")
  }

  def nameOf(c: whitebox.Context)(x: c.Expr[Any]): c.Expr[String] = {
    import c.universe._

    c.Expr[String](q"${x.tree.symbol.fullName}")
  }
}
