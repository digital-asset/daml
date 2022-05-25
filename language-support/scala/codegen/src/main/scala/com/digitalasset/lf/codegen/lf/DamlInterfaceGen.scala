// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import java.io.File

import com.daml.lf.codegen.lf.LFUtil
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref, Ref.{Identifier, QualifiedName}
import com.daml.lf.iface
import com.typesafe.scalalogging.Logger
import scalaz.syntax.std.option._

import LFUtil.domainApiAlias
import DamlContractTemplateGen.generateTemplateIdDef

import scala.reflect.runtime.universe._

object DamlInterfaceGen {
  def generate(
      util: LFUtil,
      templateId: Ref.Identifier,
      interfaceSignature: iface.DefInterface.FWT,
      companionMembers: Iterable[Tree],
  ): (File, Set[Tree], Iterable[Tree]) = {
    val damlScalaName = util.mkDamlScalaName(templateId.qualifiedName)

    val typeParent = tq"$domainApiAlias.Interface"
    val companionParent = tq"$domainApiAlias.InterfaceCompanion[${TypeName(damlScalaName.name)}]"

    val klass = q"""
      sealed abstract class ${TypeName(damlScalaName.name)} extends $typeParent"""

    val choiceMethods = genChoiceMethods(util)(damlScalaName, interfaceSignature.choices)
    logger.debug(s"TODO (#13924) interface choices $choiceMethods")

    val companion = q"""
      object ${TermName(damlScalaName.name)} extends $companionParent {
        ${generateTemplateIdDef(templateId)}
        ..$companionMembers
      }"""

    (damlScalaName.toFileName, defaultImports, Seq(klass, companion))
  }

  private val logger: Logger = Logger(getClass)

  private[this] val defaultImports = Set(LFUtil.domainApiImport)
}
