// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.lf

import java.io.File

import com.digitalasset.codegen.Util
import com.digitalasset.daml.lf.data.ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.data.Ref.{Identifier, QualifiedName}
import com.typesafe.scalalogging.Logger

import scala.reflect.runtime.universe._

/**
  *  This object is used for generating code that corresponds to a DAML contract template.
  *  An app user that uses these generated classes is guaranteed to have the same level of type
  *  safety that DAML provides.
  *
  *  See the comments below for more details on what classes/methods/types are generated.
  */
object DamlContractTemplateGen {
  import LFUtil.{domainApiAlias, rpcValueAlias}

  private val logger: Logger = Logger(getClass)

  def generate(
      util: LFUtil,
      templateId: Identifier,
      templateInterface: DefTemplateWithRecord.FWT,
      companionMembers: Iterable[Tree]): (File, Iterable[Tree]) = {

    val templateName = util.mkDamlScalaName(Util.Template, templateId)
    val contractName = util.mkDamlScalaName(Util.Contract, templateId)

    logger.debug(s"generate templateDecl: $templateName, $templateInterface")

    val templateChoiceMethods = templateInterface.template.choices.flatMap {
      case (id, interface) => util.genTemplateChoiceMethods(id, interface)
    }

    def toNamedArgumentsMethod =
      q"""
        override def toNamedArguments(` self`: ${TypeName(contractName.name)}) =
          ${util.toNamedArgumentsMap(templateInterface.`type`.fields.toList, Some(q"` self`"))}
      """

    def fromNamedArgumentsMethod = {
      import templateInterface.`type`.fields
      val typeObjectCase =
        if (fields.isEmpty) q"_root_.scala.Some(${TermName(templateName.name)}())"
        else {
          val args = LFUtil.generateIds(fields.size, "z")
          util.genForComprehensionBodyOfReaderMethod(fields, args, " r", q"""${TermName(
            templateName.name)}(..$args)""")
        }
      q"""
        override def fromNamedArguments(` r`: $rpcValueAlias.Record) = $typeObjectCase
      """
    }

    def consumingChoicesMethod = LFUtil.genConsumingChoicesMethod(templateInterface.template)

    val Identifier(_, QualifiedName(moduleName, baseName)) = templateId
    val packageIdRef = PackageIDsGen.reference(util)(moduleName)

    def templateObjectMembers = Seq(
      q"override val id = ` templateId`(packageId=$packageIdRef, moduleName=${moduleName.dottedName}, entityName=${baseName.dottedName})",
      q"""implicit final class ${TypeName(s"${contractName.name} syntax")}(private val id: $domainApiAlias.Primitive.ContractId[${TypeName(
        contractName.name)}]) extends _root_.scala.AnyVal {
            ..$templateChoiceMethods
          }""",
      consumingChoicesMethod,
      toNamedArgumentsMethod,
      fromNamedArgumentsMethod
    )

    def templateClassMembers = Seq(
      q"protected[this] override def templateCompanion(implicit ` d`: _root_.scala.Predef.DummyImplicit) = ${TermName(templateName.name)}"
    )

    DamlRecordOrVariantTypeGen.generate(
      util,
      ScopedDataType(templateId, ImmArraySeq.empty, templateInterface.`type`),
      isTemplate = true,
      rootClassChildren = templateClassMembers,
      companionChildren = templateObjectMembers ++ companionMembers
    )
  }
}
