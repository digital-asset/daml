// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import java.io.File

import com.daml.lf.data.Ref._

import scala.reflect.runtime.universe._

/** Record and variant source files all refer to this file so that
  * package ID changes during development don't force recompilation
  * of all those files.
  */
object PackageIDsGen {
  def generate(util: LFUtil): (File, Iterable[Tree]) = {

    val imports: Seq[Tree] = Seq()

    val packageIdsByModule: Map[ModuleName, PackageId] =
      (util.iface.typeDecls.keySet ++ util.iface.interfaces.keys).view
        .map(id => (id.qualifiedName.module, id.packageId))
        .toMap
    val packageIdBindings = packageIdsByModule.toSeq.sortBy(_._1.dottedName) map { case (mn, pid) =>
      q"val ${TermName(mn.dottedName)}: _root_.scala.Predef.String = $pid"
    }

    val packageIdsSrc: Tree =
      q"""
        package ${LFUtil.packageNameToRefTree(util.packageName)} {
          private[${LFUtil.packageNameTailToRefTree(util.packageName)}] object `Package IDs` {
            ..$packageIdBindings
          }
        }
       """

    val trees: Seq[Tree] = imports :+ packageIdsSrc
    val filePath = util.mkDamlScalaNameFromDirsAndName(Array(), "PackageIDs").toFileName
    filePath.getParentFile.mkdirs()
    (filePath, trees)
  }

  private[lf] def reference(moduleName: ModuleName) =
    q"`Package IDs`.${TermName(moduleName.dottedName)}"
}
