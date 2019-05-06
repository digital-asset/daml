// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.lf

import java.io.File

import com.digitalasset.codegen.Util
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.iface.reader.InterfaceType

import scala.reflect.runtime.universe._

/**
  *  This object is used for generating code that decodes incoming CreatedEvent`s
  *  from the Ledger Client API.
  *
  *  The decoder will take a CreatedEvent and return a value of type of one of the Ref
  *  inner classes in the generated contract template cases.  An in
  *
  *  A decoder for ArchivedEvent`s is not require since these don't contain any information
  *  besides the absolute contract ID of the archived contract.
  */
object EventDecoderGen {
  import LFUtil._

  def generate(util: LFUtil, supportedTemplateIds: Set[Identifier]): (File, Iterable[Tree]) = {

    val imports: Seq[Tree] = Seq(
      LFUtil.domainApiImport
    )

    def contractDamlName(alias: QualifiedName) = util.mkDamlScalaName(Util.Contract, alias)
    def contractName(alias: Identifier): RefTree = contractDamlName(alias.qualifiedName).toRefTree

    val (supportedTemplates, unsupportedTemplates) =
      util.iface.typeDecls
        .collect {
          case (qualName, InterfaceType.Template(_, _)) => qualName
        }
        .partition(supportedTemplateIds)

    // the ledger api still uses names with only dots in them, while QualifiedName.toString
    // separates the module and the name in the module with colon.
    def legacyDottedName(identifier: Identifier) = {
      val Identifier(packageId, QualifiedName(module, name)) = identifier
      s"${module.dottedName: String}.${name.dottedName: String}@${packageId: String}"
    }

    val rawUnsupportedTemplates = unsupportedTemplates.map {
      case tIdent @ Identifier(packageId, qualName) =>
        q"(${qualName.qualifiedName}, ($packageId, ${legacyDottedName(tIdent)}))"
    }

    val decoder: Tree =
      q"""
        package ${Util.packageNameToRefTree(util.packageName)} {

          object EventDecoder extends $domainApiAlias.EventDecoderApi(
            rawUnsupportedTemplates = $stdMapCompanion(..$rawUnsupportedTemplates),
            templateTypes = $stdSeqCompanion[$domainApiAlias.TemplateCompanion[_]](
                ..${supportedTemplates map contractName})
          )
        }
       """

    val trees: Seq[Tree] = imports :+ decoder
    val filePath = util.mkDamlScalaNameFromDirsAndName(Array(), "EventDecoder").toFileName
    filePath.getParentFile.mkdirs()
    (filePath, trees)
  }
}
