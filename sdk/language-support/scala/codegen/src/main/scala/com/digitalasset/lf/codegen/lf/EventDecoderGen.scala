// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import java.io.File

import com.daml.lf.data.Ref._

import scala.reflect.runtime.universe._

/**  This object is used for generating code that decodes incoming CreatedEvent`s
  *  from the Ledger Client API.
  *
  *  The decoder will take a CreatedEvent and return a value of type of one of the Ref
  *  inner classes in the generated contract template cases.  An in
  *
  *  A decoder for ArchivedEvent`s is not require since these don't contain any information
  *  besides the contract ID of the archived contract.
  */
object EventDecoderGen {
  import LFUtil._

  def generate(util: LFUtil, templateIds: Set[Identifier]): (File, Iterable[Tree]) = {

    val imports: Seq[Tree] = Seq(
      LFUtil.domainApiImport
    )

    def contractDamlName(alias: QualifiedName) = util.mkDamlScalaName(alias)
    def contractName(alias: Identifier): RefTree = contractDamlName(alias.qualifiedName).toRefTree

    val decoder: Tree =
      q"""
        package ${LFUtil.packageNameToRefTree(util.packageName)} {

          object EventDecoder extends $domainApiAlias.EventDecoderApi(
            templateTypes = $stdSeqCompanion[$domainApiAlias.TemplateCompanion[_]](
                ..${templateIds map contractName})
          )
        }
       """

    val trees: Seq[Tree] = imports :+ decoder
    val filePath = util.mkDamlScalaNameFromDirsAndName(Array(), "EventDecoder").toFileName
    filePath.getParentFile.mkdirs()
    (filePath, trees)
  }
}
