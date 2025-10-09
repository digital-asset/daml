// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package v2
package ledgerinteraction

import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.{Ast}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.toApiIdentifier
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2

import scala.util.control.NoStackTrace

sealed abstract class SubmitError
    extends RuntimeException
    with NoStackTrace
    with Product
    with Serializable {
  // Implementing code needs to be kept in sync with daml-script#Error.daml
  def toDamlSubmitError(env: ScriptF.Env): SValue
}

object SubmitError {
  import ScriptF.Env
  import com.daml.script.converter.Converter._
  import com.digitalasset.daml.lf.engine.script.v2.Converter._

  final case class SubmitErrorConverters(env: ScriptF.Env) {
    def damlScriptErrorIdentifier(s: String) =
      env.scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Submit.Error", s)
    def damlScriptVariant(
        datatypeName: String,
        variantName: String,
        rank: Int,
        fields: (String, SValue)*
    ) = {
      SVariant(
        damlScriptErrorIdentifier(datatypeName),
        Name.assertFromString(variantName),
        rank,
        record(
          damlScriptErrorIdentifier(datatypeName + "." + variantName),
          fields: _*
        ),
      )
    }
    def getConstructorRank(datatypeName: String, variantName: String): Option[Int] =
      env
        .lookupVariantConstructorRank(
          damlScriptErrorIdentifier(datatypeName),
          Name.assertFromString(variantName),
        )
        .toOption

    def damlScriptError(name: String, fields: (String, SValue)*) =
      // Handling for mismatching runner and daml-script library versions, by constructing errors by name, not by rank
      getConstructorRank("SubmitError", name) match {
        case Some(rank) => damlScriptVariant("SubmitError", name, rank, fields: _*)
        case None => {
          val unknownRank = getConstructorRank("SubmitError", "UnknownError").getOrElse(
            throw new IllegalArgumentException("Daml-Script missing SubmitError.UnknownError")
          )
          damlScriptVariant(
            "SubmitError",
            "UnknownError",
            unknownRank,
            (
              "unknownErrorMessage",
              SText(s"Outdated daml-script library failed to represent $name error as SubmitError"),
            ),
          )
        }
      }
  }

  def globalKeyToAnyContractKey(env: Env, key: GlobalKey): SValue = {
    val ty = env.lookupKeyTy(key.templateId).toOption.get
    val sValue = env.translateValue(ty, key.key).toOption.get
    fromAnyContractKey(AnyContractKey(key.templateId, ty, sValue))
  }

  def fromNonEmptySet[A](set: NonEmpty[Seq[A]], conv: A => SValue): SValue = {
    val converted: Seq[SValue] = set.map(conv)
    record(
      StablePackagesV2.NonEmpty,
      ("hd", converted.head),
      ("tl", SList(converted.tail.to(FrontStack))),
    )
  }

  final case class ContractNotFound(
      cids: NonEmpty[Seq[ContractId]],
      additionalDebuggingInfo: Option[ContractNotFound.AdditionalInfo],
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractNotFound",
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

  final case class UnresolvedPackageName(packageName: PackageName) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnresolvedPackageName",
        ("packageName", SText(packageName)),
      )
  }

  object ContractNotFound {

    sealed abstract class AdditionalInfo {
      def toSValue(env: Env): SValue
    }

    object AdditionalInfo {
      final case class NotFound() extends AdditionalInfo {
        override def toSValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotFound",
            0,
          )
      }

      final case class NotActive(
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

      final case class NotEffective(
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

      final case class NotVisible(
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

  final case class ContractKeyNotFound(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractKeyNotFound",
        ("contractKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  final case class AuthorizationError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "AuthorizationError",
        ("authorizationErrorMessage", SText(message)),
      )
  }

  final case class ContractHashingError(
      coid: ContractId,
      dstTemplateId: TypeConId,
      createArg: Value,
      message: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue = {
      SubmitErrorConverters(env).damlScriptError(
        "ContractHashingError",
        ("coid", fromAnyContractId(env.scriptIds, toApiIdentifier(dstTemplateId), coid)),
        ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
        ("createArg", fromAnyTemplate(env.valueTranslator, dstTemplateId, createArg).toOption.get),
        ("contractHashingErrorMessage", SText(message)),
      )
    }
  }

  final case class DisclosedContractKeyHashingError(
      contractId: ContractId,
      key: GlobalKey,
      givenKeyHash: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "DisclosedContractKeyHashingError",
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
        ("duplicateContractKey", SOptional(oKey.map(globalKeyToAnyContractKey(env, _)))),
      )
  }

  final case class InconsistentContractKey(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "InconsistentContractKey",
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
        ("exc", SOptional(sValue)),
      )
    }
  }

  final case class UserError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UserError",
        ("userErrorMessage", SText(message)),
      )
  }

  final case class TemplatePreconditionViolated() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TemplatePreconditionViolated"
      )
  }

  final case class CreateEmptyContractKeyMaintainers(templateId: Identifier, templateArg: Value)
      extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "CreateEmptyContractKeyMaintainers",
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
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
        ("templateId", fromTemplateTypeRep(toApiIdentifier(templateId))),
        ("requiredInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiredInterfaceId))),
        ("requiringInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiringInterfaceId))),
      )
  }

  final case class NonComparableValues() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "NonComparableValues"
      )
  }

  final case class ContractIdInContractKey() extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdInContractKey"
      )
  }

  final case class ContractIdComparability(contractId: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdComparability",
        ("globalExistingContractId", SText(contractId)),
      )
  }

  final case class ValueNesting(limit: Int) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "ValueNesting",
        ("limit", SInt64(limit.toLong)),
      )
  }

  final case class LocalVerdictLockedContracts(cids: Seq[(Identifier, ContractId)])
      extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedContracts",
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

  final case class LocalVerdictLockedKeys(keys: Seq[GlobalKey]) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedKeys",
        (
          "localVerdictLockedKeys",
          SList(keys.map(globalKeyToAnyContractKey(env, _)).to(FrontStack)),
        ),
      )
  }

  object UpgradeError {
    private def damlScriptUpgradeErrorType(
        env: Env,
        variantName: String,
        rank: Int,
        fields: (String, SValue)*
    ): SVariant =
      SubmitErrorConverters(env).damlScriptVariant(
        "Daml.Script.Internal.Questions.Submit.Error.UpgradeErrorType",
        variantName,
        rank,
        fields: _*
      )

    sealed case class ValidationFailed(
        coid: ContractId,
        srcTemplateId: Identifier,
        dstTemplateId: Identifier,
        srcPackageName: PackageName,
        dstPackageName: PackageName,
        originalSignatories: Set[Party],
        originalObservers: Set[Party],
        originalOptKey: Option[GlobalKeyWithMaintainers],
        recomputedSignatories: Set[Party],
        recomputedObservers: Set[Party],
        recomputedOptKey: Option[GlobalKeyWithMaintainers],
        message: String,
    ) extends SubmitError {
      override def toDamlSubmitError(env: Env): SValue = {
        val upgradeErrorType =
          damlScriptUpgradeErrorType(
            env,
            "ValidationFailed",
            0,
            ("coid", fromAnyContractId(env.scriptIds, toApiIdentifier(srcTemplateId), coid)),
            ("srcTemplateId", fromTemplateTypeRep(srcTemplateId)),
            ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
            ("srcPackageName", SText(srcPackageName)),
            ("dstPackageName", SText(dstPackageName)),
            (
              "originalSignatories",
              SList(originalSignatories.toList.map(SParty).to(FrontStack)),
            ),
            ("originalObservers", SList(originalObservers.toList.map(SParty).to(FrontStack))),
            (
              "originalOptKey",
              SOptional(originalOptKey.map(key => {
                val globalKey = globalKeyToAnyContractKey(env, key.globalKey)
                val maintainers = SList(key.maintainers.toList.map(SParty).to(FrontStack))
                makeTuple(globalKey, maintainers)
              })),
            ),
            (
              "recomputedSignatories",
              SList(recomputedSignatories.toList.map(SParty).to(FrontStack)),
            ),
            ("recomputedObservers", SList(recomputedObservers.toList.map(SParty).to(FrontStack))),
            (
              "recomputedOptKey",
              SOptional(recomputedOptKey.map(key => {
                val globalKey = globalKeyToAnyContractKey(env, key.globalKey)
                val maintainers = SList(key.maintainers.toList.map(SParty).to(FrontStack))
                makeTuple(globalKey, maintainers)
              })),
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          ("errorType", upgradeErrorType),
          ("errorMessage", SText(message)),
        )
      }
    }

    sealed case class TranslationFailed(
        coid: Option[ContractId],
        srcTemplateId: Identifier,
        dstTemplateId: Identifier,
        createArg: Value,
        message: String,
    ) extends SubmitError {
      override def toDamlSubmitError(env: Env): SValue = {
        val upgradeErrorType =
          damlScriptUpgradeErrorType(
            env,
            "TranslationFailed",
            1,
            (
              "coid",
              SOptional.apply(
                coid.map(id => fromAnyContractId(env.scriptIds, toApiIdentifier(srcTemplateId), id))
              ),
            ),
            ("srcTemplateId", fromTemplateTypeRep(srcTemplateId)),
            ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
            (
              "createArg",
              fromAnyTemplate(env.valueTranslator, srcTemplateId, createArg).toOption.get,
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          ("errorType", upgradeErrorType),
          ("errorMessage", SText(message)),
        )
      }
    }

    sealed case class AuthenticationFailed(
        coid: ContractId,
        srcTemplateId: Identifier,
        dstTemplateId: Identifier,
        createArg: Value,
        message: String,
    ) extends SubmitError {
      override def toDamlSubmitError(env: Env): SValue = {
        val upgradeErrorType =
          damlScriptUpgradeErrorType(
            env,
            "AuthenticationFailed",
            2,
            ("coid", fromAnyContractId(env.scriptIds, toApiIdentifier(srcTemplateId), coid)),
            ("srcTemplateId", fromTemplateTypeRep(srcTemplateId)),
            ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
            (
              "createArg",
              fromAnyTemplate(env.valueTranslator, srcTemplateId, createArg).toOption.get,
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          ("errorType", upgradeErrorType),
          ("errorMessage", SText(message)),
        )
      }
    }
  }

  final case class FailureStatusError(
      failureStatus: IE.FailureStatus,
      exerciseTrace: Option[String],
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "FailureStatusError",
        (
          "failureStatus",
          record(
            StablePackagesV2.FailureStatus,
            ("errorId", SText(failureStatus.errorId)),
            ("category", SInt64(failureStatus.failureCategory.toLong)),
            ("message", SText(failureStatus.errorMessage)),
            (
              "meta",
              SMap(true, failureStatus.metadata.map { case (k, v) => (SText(k), SText(v)) }),
            ),
          ),
        ),
      )
  }

  object CryptoError {
    final case class MalformedByteEncoding(value: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env): SValue = {
        val errorType =
          damlScriptCryptoErrorType(env, "MalformedByteEncoding", 0, "value" -> SText(value))

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", SText(message)),
        )
      }
    }

    final case class MalformedKey(key: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env): SValue = {
        val errorType = damlScriptCryptoErrorType(env, "MalformedKey", 1, "keyValue" -> SText(key))

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", SText(message)),
        )
      }
    }

    final case class MalformedSignature(signature: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env): SValue = {
        val errorType = damlScriptCryptoErrorType(
          env,
          "MalformedSignature",
          2,
          "signatureValue" -> SText(signature),
        )

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", SText(message)),
        )
      }
    }

    final case class MalformedContractId(value: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env): SValue = {
        val errorType = damlScriptCryptoErrorType(
          env,
          "MalformedContractId",
          2,
          "contractIdValue" -> SText(value),
        )

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", SText(message)),
        )
      }
    }

    private def damlScriptCryptoErrorType(
        env: Env,
        variantName: String,
        rank: Int,
        fields: (String, SValue)*
    ): SVariant =
      SubmitErrorConverters(env).damlScriptVariant(
        "Daml.Script.Internal.Questions.Submit.Error.CryptoErrorType",
        variantName,
        rank,
        fields: _*
      )
  }

  final case class DevError(errorType: String, message: String) extends SubmitError {
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
        case _ => SEnum(devErrorTypeIdentifier, Name.assertFromString("UnknownNewFeature"), 1)
      }
      SubmitErrorConverters(env).damlScriptError(
        "DevError",
        ("devErrorType", devErrorType),
        ("devErrorMessage", SText(message)),
      )
    }
  }

  final case class UnknownError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnknownError",
        ("unknownErrorMessage", SText(message)),
      )
  }

  final case class TruncatedError(errType: String, message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): SValue =
      SubmitErrorConverters(env).damlScriptError(
        "TruncatedError",
        ("truncatedErrorType", SText(errType)),
        ("truncatedErrorMessage", SText(message)),
      )
  }
}
