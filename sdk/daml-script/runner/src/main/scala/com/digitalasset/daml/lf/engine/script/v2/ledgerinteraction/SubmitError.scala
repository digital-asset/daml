// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package v2
package ledgerinteraction

import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref.{Identifier, Name}
import com.daml.lf.language.{Ast, LanguageMajorVersion, StablePackagesV2}
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.nonempty.NonEmpty
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier
import com.daml.lf.data.Ref._
import com.daml.lf.data.Time

import scala.util.control.NoStackTrace

sealed abstract class SubmitError
    extends RuntimeException
    with NoStackTrace
    with Product
    with Serializable {
  // Implementing code needs to be kept in sync with daml-script#Error.daml
  def toDamlSubmitError(env: ScriptF.Env): SValue
}

class SubmitErrors(majorLanguageVersion: LanguageMajorVersion) {
  import ScriptF.Env
  import com.daml.script.converter.Converter._

  val converter = new Converter(majorLanguageVersion)
  import converter._

  case class SubmitErrorConverters(env: ScriptF.Env) {
    def damlScriptErrorIdentifier(s: String) =
      env.scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Submit.Error", s)
    def damlScriptVariant(
        datatypeName: String,
        variantName: String,
        rank: Int,
        fields: (String, SValue)*
    ) =
      SVariant(
        damlScriptErrorIdentifier(datatypeName),
        Name.assertFromString(variantName),
        rank,
        record(
          damlScriptErrorIdentifier(datatypeName + "." + variantName),
          fields: _*
        ),
      )
    def damlScriptError(name: String, rank: Int, fields: (String, SValue)*) =
      damlScriptVariant("SubmitError", name, rank, fields: _*)
  }

  def globalKeyToAnyContractKey(env: Env, key: GlobalKey): SValue = {
    globalKeyToAnyContractKey(env, key.templateId, key.key)
  }

  def globalKeyToAnyContractKey(env: Env, templateId: Identifier, key: Value): SValue = {
    val ty = env.lookupKeyTy(templateId).toOption.get
    val sValue = env.translateValue(ty, key).toOption.get
    fromAnyContractKey(AnyContractKey(templateId, ty, sValue))
  }

  def fromNonEmptySet[A](set: NonEmpty[Seq[A]], conv: A => SValue): SValue = {
    val converted: Seq[SValue] = set.map(conv)
    record(
      StablePackagesV2.NonEmpty,
      ("hd", converted.head),
      ("tl", SList(converted.tail.to(FrontStack))),
    )
  }

  sealed case class ContractNotFound(
      cids: NonEmpty[Seq[ContractId]],
      additionalDebuggingInfo: Option[ContractNotFound.AdditionalInfo],
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractNotFound",
        0,
        (
          "unknownContractIds",
          fromNonEmptySet(cids, { cid: ContractId => SText(cid.coid) }),
        ),
        (
          "additionalDebuggingInfo",
          SOptional(additionalDebuggingInfo.map(_.toSValue(env))),
        ),
      )
  }

  object ContractNotFound {

    sealed abstract class AdditionalInfo {
      def toSValue(env: Env): SValue
    }

    object AdditionalInfo {
      sealed case class NotFound() extends AdditionalInfo {
        override def toSValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotFound",
            0,
          )
      }

      sealed case class NotActive(
          cid: ContractId,
          tid: Identifier,
      ) extends AdditionalInfo {
        override def toSValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotActive",
            1,
            (
              "additionalInfoCid",
              fromAnyContractId(env.scriptIds, toApiIdentifier(tid), cid),
            ),
          )
      }

      sealed case class NotEffective(
          cid: ContractId,
          tid: Identifier,
          effectiveAt: Time.Timestamp,
      ) extends AdditionalInfo {
        override def toSValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotEffective",
            2,
            (
              "additionalInfoCid",
              fromAnyContractId(env.scriptIds, toApiIdentifier(tid), cid),
            ),
            (
              "effectiveAt",
              SText(effectiveAt.toString),
            ),
          )
      }

      sealed case class NotVisible(
          cid: ContractId,
          tid: Identifier,
          actAs: Set[Party],
          readAs: Set[Party],
          observers: Set[Party],
      ) extends AdditionalInfo {
        override def toSValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotVisible",
            3,
            (
              "additionalInfoCid",
              fromAnyContractId(env.scriptIds, toApiIdentifier(tid), cid),
            ),
            (
              "actAs",
              SList(actAs.toList.map(SParty).to(FrontStack)),
            ),
            (
              "readAs",
              SList(readAs.toList.map(SParty).to(FrontStack)),
            ),
            (
              "observers",
              SList(observers.toList.map(SParty).to(FrontStack)),
            ),
          )
      }
    }
  }

  sealed case class ContractKeyNotFound(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractKeyNotFound",
        1,
        ("contractKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  sealed case class AuthorizationError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "AuthorizationError",
        2,
        ("authorizationErrorMessage", SText(message)),
      )
  }

  sealed case class DisclosedContractKeyHashingError(
      contractId: ContractId,
      key: GlobalKey,
      givenKeyHash: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "DisclosedContractKeyHashingError",
        3,
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(key.templateId), contractId),
        ),
        ("expectedKey", globalKeyToAnyContractKey(env, key)),
        ("givenKeyHash", SText(givenKeyHash)),
      )
  }

  sealed case class DuplicateContractKey(oKey: Option[GlobalKey]) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "DuplicateContractKey",
        4,
        ("duplicateContractKey", SOptional(oKey.map(globalKeyToAnyContractKey(env, _)))),
      )
  }

  sealed case class InconsistentContractKey(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "InconsistentContractKey",
        5,
        ("contractKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  sealed case class UnhandledException(exc: Option[(Identifier, Value)]) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val sValue = exc.map { case (ty, value) =>
        SAny(Ast.TTyCon(ty), env.translateValue(Ast.TTyCon(ty), value).toOption.get)
      }
      SubmitErrorConverters(env).damlScriptError(
        "UnhandledException",
        6,
        ("exc", SOptional(sValue)),
      )
    }
  }

  sealed case class UserError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UserError",
        7,
        ("userErrorMessage", SText(message)),
      )
  }

  sealed case class TemplatePreconditionViolated() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TemplatePreconditionViolated",
        8,
      )
  }

  sealed case class CreateEmptyContractKeyMaintainers(templateId: Identifier, templateArg: Value)
      extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "CreateEmptyContractKeyMaintainers",
        9,
        (
          "invalidTemplate",
          fromAnyTemplate(env.valueTranslator, templateId, templateArg).toOption.get,
        ),
      )
  }

  sealed case class FetchEmptyContractKeyMaintainers(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "FetchEmptyContractKeyMaintainers",
        10,
        ("failedTemplateKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  sealed case class WronglyTypedContract(
      contractId: ContractId,
      expectedTemplateId: Identifier,
      actualTemplateId: Identifier,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "WronglyTypedContract",
        11,
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(actualTemplateId), contractId),
        ),
        ("expectedTemplateId", fromTemplateTypeRep(toApiIdentifier(expectedTemplateId))),
        ("actualTemplateId", fromTemplateTypeRep(toApiIdentifier(actualTemplateId))),
      )
  }

  sealed case class ContractDoesNotImplementInterface(
      contractId: ContractId,
      templateId: Identifier,
      interfaceId: Identifier,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractDoesNotImplementInterface",
        12,
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
        ("templateId", fromTemplateTypeRep(toApiIdentifier(templateId))),
        ("interfaceId", fromTemplateTypeRep(toApiIdentifier(interfaceId))),
      )
  }

  sealed case class ContractDoesNotImplementRequiringInterface(
      contractId: ContractId,
      templateId: Identifier,
      requiredInterfaceId: Identifier,
      requiringInterfaceId: Identifier,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractDoesNotImplementInterface",
        13,
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
        ("templateId", fromTemplateTypeRep(toApiIdentifier(templateId))),
        ("requiredInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiredInterfaceId))),
        ("requiringInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiringInterfaceId))),
      )
  }

  sealed case class NonComparableValues() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "NonComparableValues",
        14,
      )
  }

  sealed case class ContractIdInContractKey() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdInContractKey",
        15,
      )
  }

  sealed case class ContractIdComparability(contractId: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdComparability",
        16,
        ("globalExistingContractId", SText(contractId)),
      )
  }

  sealed case class ValueNesting(limit: Int) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ValueNesting",
        17,
        ("limit", SInt64(limit.toLong)),
      )
  }

  sealed case class LocalVerdictLockedContracts(cids: Seq[(Identifier, ContractId)])
      extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedContracts",
        18,
        (
          "localVerdictLockedContracts",
          SList(
            cids
              .map { case (tid, cid) =>
                fromAnyContractId(env.scriptIds, toApiIdentifier(tid), cid)
              }
              .to(FrontStack)
          ),
        ),
      )
  }

  sealed case class LocalVerdictLockedKeys(keys: Seq[GlobalKey]) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedKeys",
        19,
        (
          "localVerdictLockedKeys",
          SList(keys.map(globalKeyToAnyContractKey(env, _)).to(FrontStack)),
        ),
      )
  }

  sealed case class DevError(errorType: String, message: String) extends SubmitError {
    // This code needs to be kept in sync with daml-script#Error.daml
    override def toDamlSubmitError(env: Env): SValue = {
      val devErrorTypeIdentifier =
        env.scriptIds.damlScriptModule(
          "Daml.Script.Internal.Questions.Submit.Error",
          "DevErrorType",
        )
      val devErrorType = errorType match {
        case "ChoiceGuardFailed" =>
          SEnum(devErrorTypeIdentifier, Name.assertFromString("ChoiceGuardFailed"), 0)
        case "WronglyTypedContractSoft" =>
          SEnum(devErrorTypeIdentifier, Name.assertFromString("WronglyTypedContractSoft"), 1)
        case "Upgrade" =>
          SEnum(devErrorTypeIdentifier, Name.assertFromString("Upgrade"), 2)
        case _ => SEnum(devErrorTypeIdentifier, Name.assertFromString("UnknownNewFeature"), 3)
      }
      SubmitErrorConverters(env).damlScriptError(
        "DevError",
        20,
        ("devErrorType", devErrorType),
        ("devErrorMessage", SText(message)),
      )
    }
  }

  sealed case class UnknownError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnknownError",
        21,
        ("unknownErrorMessage", SText(message)),
      )
  }

  sealed case class TruncatedError(errType: String, message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TruncatedError",
        22,
        ("truncatedErrorType", SText(errType)),
        ("truncatedErrorMessage", SText(message)),
      )
  }
}
