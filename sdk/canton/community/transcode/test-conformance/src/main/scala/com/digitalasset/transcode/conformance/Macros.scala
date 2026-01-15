// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.conformance

import scala.quoted.*

object Macros:
  def lambdaArgNames(x: Expr[Any])(using Quotes): Expr[Seq[String]] =
    import quotes.reflect.*
    val tree = x.asTerm
    val result = scala.collection.mutable.Buffer.empty[String]
    class MyTraverser extends TreeTraverser {
      override def traverseTree(tree: Tree)(owner: Symbol): Unit = tree match
        case DefDef(_, params, _, _) =>
          params
            .flatMap(_.params)
            .collect { case ValDef(name, _, _) => name }
            .foreach(result.addOne)
        case _ =>
          super.traverseTree(tree)(owner)
    }
    MyTraverser().traverseTree(tree)(tree.symbol)
    Expr(result.toSeq)
