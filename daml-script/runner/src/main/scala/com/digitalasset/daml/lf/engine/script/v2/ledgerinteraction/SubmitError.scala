// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package v2
package ledgerinteraction

import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref.{Identifier, Name}
import com.daml.lf.language.{Ast, StablePackage}
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
  def toDamlSubmitError(env: ScriptF.Env): SValue
}

object SubmitError {
  import ScriptF.Env
  import com.daml.script.converter.Converter._
  import com.daml.lf.engine.script.v2.Converter._

  final case class SubmitErrorConverters(env: ScriptF.Env) {
    def damlScriptErrorIdentifier(s: String) =
      env.scriptIds.damlScriptModule("Daml.Script.Questions.Submit.Error", s)
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
    val ty = env.lookupKeyTy(key.templateId).toOption.get
    val sValue = env.translateValue(ty, key.key).toOption.get
    fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))
  }

  def fromNonEmptySet[A](set: NonEmpty[Seq[A]], conv: A => SValue): SValue = {
    val converted: Seq[SValue] = set.map(conv)
    record(
      StablePackage.DA.NonEmpty.Types.NonEmpty,
      ("hd", converted.head),
      ("tl", SList(converted.tail.to(FrontStack))),
    )
  }

  final case class ContractNotFound(
      cids: NonEmpty[Seq[ContractId]],
      additionalDebuggingInfo: Option[ContractNotFoundAdditionalInfo],
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

  sealed abstract class ContractNotFoundAdditionalInfo {
    def toSValue(env: Env): SValue
  }

  object ContractNotFoundAdditionalInfo {
    final case class NotFound() extends ContractNotFoundAdditionalInfo {
      override def toSValue(env: Env) =
        SubmitErrorConverters(env).damlScriptVariant(
          "ContractNotFoundAdditionalInfo",
          "NotFound",
          1,
        )
    }
    final case class NotActive() extends ContractNotFoundAdditionalInfo {
      override def toSValue(env: Env) =
        SubmitErrorConverters(env).damlScriptVariant(
          "ContractNotFoundAdditionalInfo",
          "NotActive",
          2,
        )
    }
    final case class NotEffective(
        effectiveAt: Time.Timestamp
    ) extends ContractNotFoundAdditionalInfo {
      override def toSValue(env: Env) =
        SubmitErrorConverters(env).damlScriptVariant(
          "ContractNotFoundAdditionalInfo",
          "NotEffective",
          3,
          (
            "effectiveAt",
            SText(effectiveAt.toString),
          ),
        )
    }
    final case class NotVisible(
        actAs: Set[Party],
        readAs: Set[Party],
        observers: Set[Party],
    ) extends ContractNotFoundAdditionalInfo {
      override def toSValue(env: Env) =
        SubmitErrorConverters(env).damlScriptVariant(
          "ContractNotFoundAdditionalInfo",
          "NotVisible",
          4,
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

  final case class ContractKeyNotFound(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractKeyNotFound",
        1,
        ("contractKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  final case class AuthorizationError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "AuthorizationError",
        2,
        ("authorizationErrorMessage", SText(message)),
      )
  }

  final case class ContractNotActive(templateId: Identifier, contractId: ContractId)
      extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractNotActive",
        3,
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
      )
  }

  final case class DisclosedContractKeyHashingError(
      contractId: ContractId,
      key: GlobalKey,
      givenKeyHash: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "DisclosedContractKeyHashingError",
        4,
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(key.templateId), contractId),
        ),
        ("expectedKey", globalKeyToAnyContractKey(env, key)),
        ("givenKeyHash", SText(givenKeyHash)),
      )
  }

  final case class DuplicateContractKey(oKey: Option[GlobalKey]) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "DuplicateContractKey",
        5,
        ("duplicateContractKey", SOptional(oKey.map(globalKeyToAnyContractKey(env, _)))),
      )
  }

  final case class InconsistentContractKey(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "InconsistentContractKey",
        6,
        ("contractKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  final case class UnhandledException(exc: Option[(Identifier, Value)]) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val sValue = exc.map { case (ty, value) =>
        SAny(Ast.TTyCon(ty), env.translateValue(Ast.TTyCon(ty), value).toOption.get)
      }
      SubmitErrorConverters(env).damlScriptError(
        "UnhandledException",
        7,
        ("exc", SOptional(sValue)),
      )
    }
  }

  final case class UserError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UserError",
        8,
        ("userErrorMessage", SText(message)),
      )
  }

  final case class TemplatePreconditionViolated() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TemplatePreconditionViolated",
        9,
      )
  }

  final case class CreateEmptyContractKeyMaintainers(templateId: Identifier, templateArg: Value)
      extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "CreateEmptyContractKeyMaintainers",
        10,
        (
          "invalidTemplate",
          fromAnyTemplate(env.valueTranslator, templateId, templateArg).toOption.get,
        ),
      )
  }

  final case class FetchEmptyContractKeyMaintainers(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "FetchEmptyContractKeyMaintainers",
        11,
        ("failedTemplateKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  final case class WronglyTypedContract(
      contractId: ContractId,
      expectedTemplateId: Identifier,
      actualTemplateId: Identifier,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "WronglyTypedContract",
        12,
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(actualTemplateId), contractId),
        ),
        ("expectedTemplateId", fromTemplateTypeRep(toApiIdentifier(expectedTemplateId))),
        ("actualTemplateId", fromTemplateTypeRep(toApiIdentifier(actualTemplateId))),
      )
  }

  final case class ContractDoesNotImplementInterface(
      contractId: ContractId,
      templateId: Identifier,
      interfaceId: Identifier,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractDoesNotImplementInterface",
        13,
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
        ("templateId", fromTemplateTypeRep(toApiIdentifier(templateId))),
        ("interfaceId", fromTemplateTypeRep(toApiIdentifier(interfaceId))),
      )
  }

  final case class ContractDoesNotImplementRequiringInterface(
      contractId: ContractId,
      templateId: Identifier,
      requiredInterfaceId: Identifier,
      requiringInterfaceId: Identifier,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractDoesNotImplementInterface",
        14,
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
        ("templateId", fromTemplateTypeRep(toApiIdentifier(templateId))),
        ("requiredInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiredInterfaceId))),
        ("requiringInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiringInterfaceId))),
      )
  }

  final case class NonComparableValues() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "NonComparableValues",
        15,
      )
  }

  final case class ContractIdInContractKey() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdInContractKey",
        16,
      )
  }

  final case class ContractIdComparability(contractId: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdComparability",
        17,
        ("globalExistingContractId", SText(contractId)),
      )
  }

  final case class DevError(errorType: String, message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val devErrorTypeIdentifier =
        env.scriptIds.damlScriptModule("Daml.Script.Questions.Submit.Error", "DevErrorType")
      val devErrorType = errorType match {
        case "ChoiceGuardFailed" =>
          SEnum(devErrorTypeIdentifier, Name.assertFromString("ChoiceGuardFailed"), 0)
        case "WronglyTypedContractSoft" =>
          SEnum(devErrorTypeIdentifier, Name.assertFromString("WronglyTypedContractSoft"), 1)
        case "Limit" => SEnum(devErrorTypeIdentifier, Name.assertFromString("Limit"), 2)
        case _ => SEnum(devErrorTypeIdentifier, Name.assertFromString("UnknownNewFeature"), 3)
      }
      SubmitErrorConverters(env).damlScriptError(
        "DevError",
        18,
        ("devErrorType", devErrorType),
        ("devErrorMessage", SText(message)),
      )
    }
  }

  final case class UnknownError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnknownError",
        19,
        ("unknownErrorMessage", SText(message)),
      )
  }

  final case class TruncatedError(errType: String, message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TruncatedError",
        20,
        ("truncatedErrorType", SText(errType)),
        ("truncatedErrorMessage", SText(message)),
      )
  }
}
