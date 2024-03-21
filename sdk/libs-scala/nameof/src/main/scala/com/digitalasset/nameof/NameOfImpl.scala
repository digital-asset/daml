// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nameof

import scala.annotation.tailrec
import scala.reflect.macros.blackbox.Context

object NameOfImpl {
  def qualifiedNameOfCurrentFunc(c: Context): c.Expr[String] = {
    import c.universe._
    val cc = c.asInstanceOf[scala.reflect.macros.runtime.Context]
    val owner = cc.callsiteTyper.context.enclMethod.owner
    if (owner == NoSymbol)
      c.abort(c.enclosingPosition, "qualifiedNameOfCurrentFunc can be used only inside functions.")
    c.Expr[String](q"${owner.fullName}")
  }

  def qualifiedNameOfMember[A](c: Context)(func: c.Expr[A => Any]): c.Expr[String] =
    qualifiedNameOfTree(c)("qualifiedNameOfMember", func.tree)

  def qualifiedNameOf(c: Context)(x: c.Expr[Any]): c.Expr[String] =
    qualifiedNameOfTree(c)("qualifiedNameOf", x.tree)

  private def qualifiedNameOfTree(
      c: Context
  )(macroName: String, tree: c.universe.Tree): c.Expr[String] = {
    import c.universe._

    @tailrec def stripFunctionApply(tree: Tree): Tree = {
      tree match {
        case Function(_, body) => stripFunctionApply(body)
        case Apply(target, _) => stripFunctionApply(target)
        case TypeApply(target, _) => stripFunctionApply(target)
        case _ => tree
      }
    }

    val symbol = stripFunctionApply(tree).symbol
    if ((symbol eq null) || (symbol == NoSymbol))
      c.abort(c.enclosingPosition, s"$macroName could not resolve symbol")
    c.Expr[String](q"""${symbol.fullName}""")
  }

}
