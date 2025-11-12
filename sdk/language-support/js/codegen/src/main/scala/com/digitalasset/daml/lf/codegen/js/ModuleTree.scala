// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen.js

import com.digitalasset.daml.lf.data.Ref.{ModuleName, Name}

// The module structure of a Daml package can have "holes", i.e.,
// you can have modules `A` and `A.B.C` but no module `A.B`. We call such a
// module `A.B` a "virtual module". In order to use ES2015 modules and form
// a hierarchy of these, we need to produce JavaScript modules for virtual
// Daml modules as well. To this end, we assemble the names of all modules
// into a tree structure where each node is marked whether is is virtual or
// not. Afterwards, we take this tree structure and write a resembling
// directory structure full of `index.ts` files to disk.
private[codegen] final case class ModuleTree(
    isVirtual: Boolean,
    children: Map[Name, ModuleTree],
) {
  private lazy val childNames = children.keys.toSeq.sorted

  def add(module: ModuleName): ModuleTree = add(module.segments.toList)

  def add(parts: List[Name]): ModuleTree = parts match {
    case Nil => this.copy(isVirtual = false)
    case head :: tail =>
      val child = children.getOrElse(head, ModuleTree.empty).add(tail)
      this.copy(children = children + (head -> child))
  }

  def renderTsExports: String =
    childNames
      .map { name =>
        s"""|${GenHelper.renderES6Import(name, s"./$name")}
            |export { $name };""".stripMargin
      }
      .mkString("\n")

  def renderJsExports: String =
    childNames
      .map { name =>
        s"""|${GenHelper.renderES5Import(name, s"./$name")}
            |exports.$name = $name;""".stripMargin
      }
      .mkString("\n")
}

private[codegen] object ModuleTree {
  val empty = ModuleTree(isVirtual = true, Map.empty)
}
