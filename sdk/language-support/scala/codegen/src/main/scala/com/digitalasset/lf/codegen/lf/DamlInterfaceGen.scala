// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import java.io.File

import com.daml.lf.data.Ref
import com.daml.lf.typesig
import com.typesafe.scalalogging.Logger

import LFUtil.domainApiAlias
import DamlContractTemplateGen.{genChoiceImplicitClass, generateTemplateIdDef}

import scala.reflect.runtime.universe._

object DamlInterfaceGen {
  type DataType = typesig.DefInterface.FWT

  def generate(
      util: LFUtil,
      templateId: Ref.Identifier,
      interfaceSignature: DataType,
      companionMembers: Iterable[Tree],
  ): (File, Set[Tree], Iterable[Tree]) = {
    val damlScalaName = util.mkDamlScalaName(templateId.qualifiedName)

    logger.debug(
      s"generate interfaceDecl: ${damlScalaName.toString}, ${interfaceSignature.toString}"
    )

    val typeParent = tq"$domainApiAlias.Interface"
    val companionParent = tq"$domainApiAlias.InterfaceCompanion[${TypeName(damlScalaName.name)}]"

    val klass = q"""
      sealed abstract class ${TypeName(damlScalaName.name)} extends $typeParent"""

    val companion = q"""
      object ${TermName(damlScalaName.name)} extends $companionParent {
        ${generateTemplateIdDef(templateId)}
        ${genChoiceImplicitClass(util)(damlScalaName, interfaceSignature.choices)}
        ..$companionMembers
      }"""

    (damlScalaName.toFileName, defaultImports, Seq(klass, companion))
  }

  private val logger: Logger = Logger(getClass)

  private[this] val defaultImports = Set(LFUtil.domainApiImport)
}
