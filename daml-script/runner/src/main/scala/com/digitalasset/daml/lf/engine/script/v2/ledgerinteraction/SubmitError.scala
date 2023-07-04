// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package v2
package ledgerinteraction

import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref.{Identifier, Name, Party}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import com.daml.lf.speedy.SValue._
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.platform.participant.util.LfEngineToApi.toApiIdentifier

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
    def damlScriptError(name: String, rank: Int, fields: (String, SValue)*) =
      SVariant(
        damlScriptErrorIdentifier("SubmitError"),
        Name.assertFromString(name),
        rank,
        record(
          damlScriptErrorIdentifier("SubmitError." + name),
          fields: _*
        ),
      )
  }

  final case class ContractNotFound(cid: ContractId) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractNotFound",
        0,
        (
          "contractId",
          SText(cid.coid),
        ), // TODO: Theres probably a better stringification method, we want to show it how daml does. Look at fromContractId
      )
  }

  final case class ContractKeyNotFound(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val ty = env.lookupKeyTy(key.templateId).toOption.get
      val sValue = env.translateValue(ty, key.key).toOption.get
      SubmitErrorConverters(env).damlScriptError(
        "ContractKeyNotFound",
        1,
        ("contractKey", fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))),
      )
    }
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
    override def toDamlSubmitError(env: Env): SValue = {
      val ty = env.lookupKeyTy(key.templateId).toOption.get
      val sValue = env.translateValue(ty, key.key).toOption.get
      SubmitErrorConverters(env).damlScriptError(
        "DisclosedContractKeyHashingError",
        4,
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(key.templateId), contractId),
        ),
        ("expectedKey", fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))),
        ("givenKeyHash", SText(givenKeyHash)),
      )
    }
  }

  final case class ContractKeyNotVisible(
      contractId: ContractId,
      key: GlobalKey,
      actAs: Seq[Party],
      readAs: Seq[Party],
      stakeholders: Seq[Party],
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val ty = env.lookupKeyTy(key.templateId).toOption.get
      val sValue = env.translateValue(ty, key.key).toOption.get
      SubmitErrorConverters(env).damlScriptError(
        "ContractKeyNotVisible",
        5,
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(key.templateId), contractId),
        ),
        ("contractKey", fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))),
        ("actAs", SList(actAs.map(SParty).to(FrontStack))),
        ("readAs", SList(readAs.map(SParty).to(FrontStack))),
        ("stakeholders", SList(stakeholders.map(SParty).to(FrontStack))),
      )
    }
  }

  final case class DuplicateContractKey(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val ty = env.lookupKeyTy(key.templateId).toOption.get
      val sValue = env.translateValue(ty, key.key).toOption.get
      SubmitErrorConverters(env).damlScriptError(
        "DuplicateContractKey",
        6,
        ("contractKey", fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))),
      )
    }
  }

  final case class InconsistentContractKey(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val ty = env.lookupKeyTy(key.templateId).toOption.get
      val sValue = env.translateValue(ty, key.key).toOption.get
      SubmitErrorConverters(env).damlScriptError(
        "InconsistentContractKey",
        7,
        ("contractKey", fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))),
      )
    }
  }

  final case class UnhandledException(exc: Option[(Identifier, Value)]) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val sValue = exc.map { case (ty, value) =>
        SAny(Ast.TTyCon(ty), env.translateValue(Ast.TTyCon(ty), value).toOption.get)
      }
      SubmitErrorConverters(env).damlScriptError(
        "UnhandledException",
        8,
        ("exc", SOptional(sValue)),
      )
    }
  }

  final case class UserError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UserError",
        9,
        ("userErrorMessage", SText(message)),
      )
  }

  final case class TemplatePreconditionViolated() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TemplatePreconditionViolated",
        10,
      )
  }

  final case class CreateEmptyContractKeyMaintainers(templateId: Identifier, templateArg: Value)
      extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "CreateEmptyContractKeyMaintainers",
        11,
        (
          "invalidTemplate",
          fromAnyTemplate(env.valueTranslator, templateId, templateArg).toOption.get,
        ),
      )
  }

  final case class FetchEmptyContractKeyMaintainers(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      val ty = env.lookupKeyTy(key.templateId).toOption.get
      val sValue = env.translateValue(ty, key.key).toOption.get
      SubmitErrorConverters(env).damlScriptError(
        "FetchEmptyContractKeyMaintainers",
        12,
        ("failedTemplateKey", fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))),
      )
    }
  }

  final case class WronglyTypedContract(
      contractId: ContractId,
      expectedTemplateId: Identifier,
      actualTemplateId: Identifier,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "WronglyTypedContract",
        13,
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
        14,
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
        15,
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
        16,
      )
  }

  final case class ContractIdInContractKey() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdInContractKey",
        17,
      )
  }

  final case class ContractIdComparability(contractId: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdComparability",
        18,
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
        19,
        ("devErrorType", devErrorType),
        ("devErrorMessage", SText(message)),
      )
    }
  }

  final case class UnknownError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnknownError",
        20,
        ("unknownErrorMessage", SText(message)),
      )
  }

  final case class TruncatedError(errType: String, message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TruncatedError",
        21,
        ("truncatedErrorType", SText(errType)),
        ("truncatedErrorMessage", SText(message)),
      )
  }
}
