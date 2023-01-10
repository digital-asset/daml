// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.lf

import java.io.File

import com.daml.lf.codegen.lf.LFUtil
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref, Ref.{Identifier, QualifiedName}
import com.daml.lf.typesig
import com.typesafe.scalalogging.Logger
import scalaz.syntax.std.option._

import scala.reflect.runtime.universe._

/**  This object is used for generating code that corresponds to a Daml contract template.
  *  An app user that uses these generated classes is guaranteed to have the same level of type
  *  safety that Daml provides.
  *
  *  See the comments below for more details on what classes/methods/types are generated.
  */
object DamlContractTemplateGen {
  import LFUtil.{domainApiAlias, rpcValueAlias}

  private val logger: Logger = Logger(getClass)

  def generate(
      util: LFUtil,
      templateId: Identifier,
      templateInterface: DefTemplateWithRecord,
      companionMembers: Iterable[Tree],
  ): (File, Set[Tree], Iterable[Tree]) = {

    val templateName = util.mkDamlScalaName(templateId.qualifiedName)

    logger.debug(s"generate templateDecl: ${templateName.toString}, ${templateInterface.toString}")

    def toNamedArgumentsMethod =
      q"""
        override def toNamedArguments(` self`: ${TypeName(templateName.name)}) =
          ${util.toNamedArgumentsMap(templateInterface.`type`.fields.toList, Some(q"` self`"))}
      """

    def fromNamedArgumentsMethod = {
      import templateInterface.`type`.fields
      val typeObjectCase =
        if (fields.isEmpty) q"_root_.scala.Some(${TermName(templateName.name)}())"
        else {
          val args = LFUtil.generateIds(fields.size, "z")
          util.genForComprehensionBodyOfReaderMethod(
            fields,
            args,
            " r",
            q"""${TermName(templateName.name)}(..$args)""",
          )
        }
      q"""
        override def fromNamedArguments(` r`: $rpcValueAlias.Record) = $typeObjectCase
      """
    }

    def consumingChoicesMethod = LFUtil.genConsumingChoicesMethod(templateInterface.template)

    def implementedInterfaceProof = templateInterface.template.implementedInterfaces map { ifn =>
      val ifSn = util.mkDamlScalaName(ifn.qualifiedName)
      q"""implicit val ${TermName(s"implements ${ifSn.qualifiedTermName}")}
         : $domainApiAlias.Template.Implements[${TypeName(templateName.name)},
                                               ${ifSn.qualifiedTypeName}] =
         new $domainApiAlias.Template.Implements"""
    }

    def templateObjectMembers = Seq(
      generateTemplateIdDef(templateId),
      genChoiceImplicitClass(util)(
        templateName,
        // TODO (#13926) replace assumeNoOverloadedChoices with directChoices
        templateInterface.template.tChoices.assumeNoOverloadedChoices(githubIssue = 13926),
      ),
      q"type key = ${templateInterface.template.key.cata(util.genTypeToScalaType, LFUtil.nothingType)}",
      consumingChoicesMethod,
      toNamedArgumentsMethod,
      fromNamedArgumentsMethod,
    ) ++ implementedInterfaceProof

    def templateClassMembers = Seq(
      q"protected[this] override def templateCompanion(implicit ` d`: $domainApiAlias.Compat.DummyImplicit) = ${TermName(templateName.name)}"
    )

    DamlDataTypeGen.generate(
      util,
      ScopedDataType(templateId, ImmArraySeq.empty, templateInterface.`type`),
      isTemplate = true,
      rootClassChildren = templateClassMembers,
      companionChildren = templateObjectMembers ++ companionMembers,
    )
  }

  private val syntaxIdDecl = LFUtil.toCovariantTypeDef(" ExOn")
  private val syntaxIdType = TypeName(" ExOn")

  private[lf] def genChoiceImplicitClass(
      util: LFUtil
  )(templateName: util.DamlScalaName, choices: Map[Ref.ChoiceName, typesig.TemplateChoice.FWT]) = {
    val templateChoiceMethods = choices.flatMap { case (id, interface) =>
      util.genTemplateChoiceMethods(
        templateType = tq"${TypeName(templateName.name)}",
        idType = syntaxIdType,
        id,
        interface,
      )
    }
    q"""implicit final class ${TypeName(
        s"${templateName.name} syntax"
      )}[$syntaxIdDecl](private val id: $syntaxIdType) extends _root_.scala.AnyVal {
        ..$templateChoiceMethods
      }"""
  }

  private[lf] def generateTemplateIdDef(templateId: Identifier) = {
    val Identifier(_, QualifiedName(moduleName, baseName)) = templateId
    val packageIdRef = PackageIDsGen.reference(moduleName)
    q"override val id = ` templateId`(packageId=$packageIdRef, moduleName=${moduleName.dottedName}, entityName=${baseName.dottedName})"
  }
}
