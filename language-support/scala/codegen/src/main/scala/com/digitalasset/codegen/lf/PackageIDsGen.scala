// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.lf

import java.io.File

import com.daml.codegen.Util
import com.daml.lf.data.Ref._

import scala.collection.breakOut
import scala.reflect.runtime.universe._

/** Record and variant source files all refer to this file so that
  * package ID changes during development don't force recompilation
  * of all those files.
  */
object PackageIDsGen {
  def generate(util: LFUtil): (File, Iterable[Tree]) = {

    val imports: Seq[Tree] = Seq()

    val packageIdsByModule: Map[ModuleName, PackageId] =
      util.iface.typeDecls.keys.map(id => (id.qualifiedName.module, id.packageId))(breakOut)
    val packageIdBindings = packageIdsByModule.toSeq.sortBy(_._1.dottedName) map {
      case (mn, pid) =>
        q"val ${TermName(mn.dottedName)}: _root_.scala.Predef.String = $pid"
    }

    val packageIdsSrc: Tree =
      q"""
        package ${Util.packageNameToRefTree(util.packageName)} {
          private object `Package IDs` {
            ..$packageIdBindings
          }
        }
       """

    val trees: Seq[Tree] = imports :+ packageIdsSrc
    val filePath = util.mkDamlScalaNameFromDirsAndName(Array(), "PackageIDs").toFileName
    filePath.getParentFile.mkdirs()
    (filePath, trees)
  }

  private[lf] def reference(util: LFUtil)(moduleName: ModuleName) =
    q"`Package IDs`.${TermName(moduleName.dottedName)}"
}
