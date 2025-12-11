// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package v2
package ledgerinteraction

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.toApiIdentifier
import com.digitalasset.daml.lf.data.{FrontStack, SortedLookupList, Time}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.ExtendedValue
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._

import scala.util.control.NoStackTrace

sealed abstract class SubmitError
    extends RuntimeException
    with NoStackTrace
    with Product
    with Serializable {
  // Implementing code needs to be kept in sync with daml-script#Error.daml
  def toDamlSubmitError(env: ScriptF.Env): ExtendedValue
}

object SubmitError {
  import ScriptF.Env
  import com.digitalasset.daml.lf.script.converter.Converter._
  import com.digitalasset.daml.lf.engine.script.v2.Converter._

  final case class SubmitErrorConverters(env: ScriptF.Env) {
    def damlScriptErrorIdentifier(s: String) =
      env.scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Submit.Error", s)
    def damlScriptVariant(
        datatypeName: String,
        variantName: String,
        fields: (String, ExtendedValue)*
    ) = {
      ValueVariant(
        Some(damlScriptErrorIdentifier(datatypeName)),
        Name.assertFromString(variantName),
        record(
          damlScriptErrorIdentifier(datatypeName + "." + variantName),
          fields: _*
        ),
      )
    }
    def doesConstructorExist(datatypeName: String, variantName: String): Boolean =
      env
        .doesVariantConstructorExist(
          damlScriptErrorIdentifier(datatypeName),
          Name.assertFromString(variantName),
        )

    def damlScriptError(name: String, fields: (String, ExtendedValue)*) =
      // Handling for mismatching runner and daml-script library versions, by constructing errors by name, not by rank
      if (doesConstructorExist("SubmitError", name))
        damlScriptVariant("SubmitError", name, fields: _*)
      else
        damlScriptVariant(
          "SubmitError",
          "UnknownError",
          (
            "unknownErrorMessage",
            ValueText(
              s"Outdated daml-script library failed to represent $name error as SubmitError"
            ),
          ),
        )
  }

  def globalKeyToAnyContractKey(env: Env, key: GlobalKey): ExtendedValue = {
    val ty = env.lookupKeyTy(key.templateId).toOption.get
    val enrichedKey = env.enricher.enrichContractKey(key.templateId, key.key).consume().toOption.get
    fromAnyContractKey(AnyContractKey(key.templateId, ty, enrichedKey))
  }

  def fromNonEmptySet[A](set: NonEmpty[Seq[A]], conv: A => ExtendedValue): ExtendedValue = {
    val converted: Seq[ExtendedValue] = set.map(conv)
    record(
      StablePackagesV2.NonEmpty,
      ("hd", converted.head),
      ("tl", ValueList(converted.tail.to(FrontStack))),
    )
  }

  final case class ContractNotFound(
      cids: NonEmpty[Seq[ContractId]],
      additionalDebuggingInfo: Option[ContractNotFound.AdditionalInfo],
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractNotFound",
        (
          "unknownContractIds",
          fromNonEmptySet(cids, { cid: ContractId => ValueText(cid.coid) }),
        ),
        (
          "additionalDebuggingInfo",
          ValueOptional(additionalDebuggingInfo.map(_.toValue(env))),
        ),
      )
  }

  final case class UnresolvedPackageName(packageName: PackageName) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnresolvedPackageName",
        ("packageName", ValueText(packageName)),
      )
  }

  object ContractNotFound {

    sealed abstract class AdditionalInfo {
      def toValue(env: Env): ExtendedValue
    }

    object AdditionalInfo {
      final case class NotFound() extends AdditionalInfo {
        override def toValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotFound",
          )
      }

      final case class NotActive(
          cid: ContractId,
          tid: Identifier,
      ) extends AdditionalInfo {
        override def toValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotActive",
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
        override def toValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotEffective",
            (
              "additionalInfoCid",
              fromAnyContractId(env.scriptIds, toApiIdentifier(tid), cid),
            ),
            (
              "effectiveAt",
              ValueText(effectiveAt.toString),
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
        override def toValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariant(
            "ContractNotFoundAdditionalInfo",
            "NotVisible",
            (
              "additionalInfoCid",
              fromAnyContractId(env.scriptIds, toApiIdentifier(tid), cid),
            ),
            (
              "actAs",
              ValueList(actAs.toList.map(ValueParty).to(FrontStack)),
            ),
            (
              "readAs",
              ValueList(readAs.toList.map(ValueParty).to(FrontStack)),
            ),
            (
              "observers",
              ValueList(observers.toList.map(ValueParty).to(FrontStack)),
            ),
          )
      }
    }
  }

  final case class ContractKeyNotFound(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractKeyNotFound",
        ("contractKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  final case class AuthorizationError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "AuthorizationError",
        ("authorizationErrorMessage", ValueText(message)),
      )
  }

  final case class ContractHashingError(
      coid: ContractId,
      dstTemplateId: TypeConId,
      createArg: Value,
      message: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue = {
      SubmitErrorConverters(env).damlScriptError(
        "ContractHashingError",
        ("coid", fromAnyContractId(env.scriptIds, toApiIdentifier(dstTemplateId), coid)),
        ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
        ("createArg", fromAnyTemplate(dstTemplateId, createArg)),
        ("contractHashingErrorMessage", ValueText(message)),
      )
    }
  }

  final case class DisclosedContractKeyHashingError(
      contractId: ContractId,
      key: GlobalKey,
      givenKeyHash: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "DisclosedContractKeyHashingError",
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(key.templateId), contractId),
        ),
        ("expectedKey", globalKeyToAnyContractKey(env, key)),
        ("givenKeyHash", ValueText(givenKeyHash)),
      )
  }

  final case class DuplicateContractKey(oKey: Option[GlobalKey]) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "DuplicateContractKey",
        ("duplicateContractKey", ValueOptional(oKey.map(globalKeyToAnyContractKey(env, _)))),
      )
  }

  final case class InconsistentContractKey(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "InconsistentContractKey",
        ("contractKey", globalKeyToAnyContractKey(env, key)),
      )
  }

  final case class UnhandledException(exc: Option[(Identifier, Value)]) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue = {
      val anyException = exc.map { case (ty, value) =>
        fromAnyException(ty, env.enricher.enrichException(ty, value).consume().toOption.get)
      }
      SubmitErrorConverters(env).damlScriptError(
        "UnhandledException",
        ("exc", ValueOptional(anyException)),
      )
    }
  }

  final case class UserError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "UserError",
        ("userErrorMessage", ValueText(message)),
      )
  }

  final case class TemplatePreconditionViolated() extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "TemplatePreconditionViolated"
      )
  }

  final case class CreateEmptyContractKeyMaintainers(templateId: Identifier, templateArg: Value)
      extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "CreateEmptyContractKeyMaintainers",
        (
          "invalidTemplate", {
            val enrichedArg =
              env.enricher.enrichContract(templateId, templateArg).consume().toOption.get
            fromAnyTemplate(templateId, enrichedArg)
          },
        ),
      )
  }

  final case class FetchEmptyContractKeyMaintainers(key: GlobalKey) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
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
    override def toDamlSubmitError(env: Env): ExtendedValue =
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
    override def toDamlSubmitError(env: Env): ExtendedValue =
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
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractDoesNotImplementInterface",
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
        ("templateId", fromTemplateTypeRep(toApiIdentifier(templateId))),
        ("requiredInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiredInterfaceId))),
        ("requiringInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiringInterfaceId))),
      )
  }

  final case class NonComparableValues() extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "NonComparableValues"
      )
  }

  final case class ContractIdInContractKey() extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdInContractKey"
      )
  }

  final case class ContractIdComparability(contractId: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdComparability",
        ("globalExistingContractId", ValueText(contractId)),
      )
  }

  final case class ValueNesting(limit: Int) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ValueNesting",
        ("limit", ValueInt64(limit.toLong)),
      )
  }

  final case class MalformedText(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "MalformedText",
        ("malformedTextMessage", ValueText(message)),
      )
  }

  final case class LocalVerdictLockedContracts(cids: Seq[(Identifier, ContractId)])
      extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedContracts",
        (
          "localVerdictLockedContracts",
          ValueList(
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
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedKeys",
        (
          "localVerdictLockedKeys",
          ValueList(keys.map(globalKeyToAnyContractKey(env, _)).to(FrontStack)),
        ),
      )
  }

  object UpgradeError {
    private def damlScriptUpgradeErrorType(
        env: Env,
        variantName: String,
        fields: (String, ExtendedValue)*
    ): ExtendedValue =
      SubmitErrorConverters(env).damlScriptVariant(
        "UpgradeErrorType",
        variantName,
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
      override def toDamlSubmitError(env: Env): ExtendedValue = {
        val upgradeErrorType =
          damlScriptUpgradeErrorType(
            env,
            "ValidationFailed",
            ("coid", fromAnyContractId(env.scriptIds, toApiIdentifier(srcTemplateId), coid)),
            ("srcTemplateId", fromTemplateTypeRep(srcTemplateId)),
            ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
            ("srcPackageName", ValueText(srcPackageName)),
            ("dstPackageName", ValueText(dstPackageName)),
            (
              "originalSignatories",
              ValueList(originalSignatories.toList.map(ValueParty).to(FrontStack)),
            ),
            (
              "originalObservers",
              ValueList(originalObservers.toList.map(ValueParty).to(FrontStack)),
            ),
            (
              "originalKeyOpt",
              ValueOptional(originalOptKey.map(key => {
                val globalKey = globalKeyToAnyContractKey(env, key.globalKey)
                val maintainers = ValueList(key.maintainers.toList.map(ValueParty).to(FrontStack))
                makeTuple(globalKey, maintainers)
              })),
            ),
            (
              "recomputedSignatories",
              ValueList(recomputedSignatories.toList.map(ValueParty).to(FrontStack)),
            ),
            (
              "recomputedObservers",
              ValueList(recomputedObservers.toList.map(ValueParty).to(FrontStack)),
            ),
            (
              "recomputedKeyOpt",
              ValueOptional(recomputedOptKey.map(key => {
                val globalKey = globalKeyToAnyContractKey(env, key.globalKey)
                val maintainers = ValueList(key.maintainers.toList.map(ValueParty).to(FrontStack))
                makeTuple(globalKey, maintainers)
              })),
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          ("errorType", upgradeErrorType),
          ("errorMessage", ValueText(message)),
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
      override def toDamlSubmitError(env: Env): ExtendedValue = {
        val upgradeErrorType =
          damlScriptUpgradeErrorType(
            env,
            "TranslationFailed",
            (
              "mCoid",
              ValueOptional.apply(
                coid.map(id => fromAnyContractId(env.scriptIds, toApiIdentifier(srcTemplateId), id))
              ),
            ),
            ("srcTemplateId", fromTemplateTypeRep(srcTemplateId)),
            ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
            (
              "createArg", {
                val enrichedArg =
                  env.enricher.enrichContract(srcTemplateId, createArg).consume().toOption.get
                fromAnyTemplate(srcTemplateId, enrichedArg)
              },
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          ("errorType", upgradeErrorType),
          ("errorMessage", ValueText(message)),
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
      override def toDamlSubmitError(env: Env): ExtendedValue = {
        val upgradeErrorType =
          damlScriptUpgradeErrorType(
            env,
            "AuthenticationFailed",
            ("coid", fromAnyContractId(env.scriptIds, toApiIdentifier(srcTemplateId), coid)),
            ("srcTemplateId", fromTemplateTypeRep(srcTemplateId)),
            ("dstTemplateId", fromTemplateTypeRep(dstTemplateId)),
            (
              "createArg", {
                val enrichedArg =
                  env.enricher.enrichContract(srcTemplateId, createArg).consume().toOption.get
                fromAnyTemplate(srcTemplateId, enrichedArg)
              },
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          ("errorType", upgradeErrorType),
          ("errorMessage", ValueText(message)),
        )
      }
    }
  }

  final case class FailureStatusError(
      failureStatus: IE.FailureStatus,
      exerciseTrace: Option[String],
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "FailureStatusError",
        (
          "failureStatus",
          record(
            StablePackagesV2.FailureStatus,
            ("errorId", ValueText(failureStatus.errorId)),
            ("category", ValueInt64(failureStatus.failureCategory.toLong)),
            ("message", ValueText(failureStatus.errorMessage)),
            (
              "meta",
              ValueTextMap(
                SortedLookupList(failureStatus.metadata.view.mapValues(ValueText(_)).toMap)
              ),
            ),
          ),
        ),
      )
  }

  object CryptoError {
    final case class MalformedByteEncoding(value: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env): ExtendedValue = {
        val errorType =
          damlScriptCryptoErrorType(env, "MalformedByteEncoding", "value" -> ValueText(value))

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", ValueText(message)),
        )
      }
    }

    final case class MalformedKey(key: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env): ExtendedValue = {
        val errorType = damlScriptCryptoErrorType(env, "MalformedKey", "keyValue" -> ValueText(key))

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", ValueText(message)),
        )
      }
    }

    final case class MalformedSignature(signature: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env): ExtendedValue = {
        val errorType = damlScriptCryptoErrorType(
          env,
          "MalformedSignature",
          "signatureValue" -> ValueText(signature),
        )

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", ValueText(message)),
        )
      }
    }

    private def damlScriptCryptoErrorType(
        env: Env,
        variantName: String,
        fields: (String, ExtendedValue)*
    ): ExtendedValue =
      SubmitErrorConverters(env).damlScriptVariant(
        "CryptoErrorType",
        variantName,
        fields: _*
      )
  }

  final case class DevError(errorType: String, message: String) extends SubmitError {
    // This code needs to be kept in sync with daml-script#Error.daml
    override def toDamlSubmitError(env: Env): ExtendedValue = {
      val devErrorTypeIdentifier =
        env.scriptIds.damlScriptModule(
          "Daml.Script.Internal.Questions.Submit.Error",
          "DevErrorType",
        )
      val devErrorType = errorType match {
        case "ChoiceGuardFailed" =>
          ValueEnum(Some(devErrorTypeIdentifier), Name.assertFromString("ChoiceGuardFailed"))
        case _ =>
          ValueEnum(Some(devErrorTypeIdentifier), Name.assertFromString("UnknownNewFeature"))
      }
      SubmitErrorConverters(env).damlScriptError(
        "DevError",
        ("devErrorType", devErrorType),
        ("devErrorMessage", ValueText(message)),
      )
    }
  }

  final case class UnknownError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnknownError",
        ("unknownErrorMessage", ValueText(message)),
      )
  }

  final case class TruncatedError(errType: String, message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "TruncatedError",
        ("truncatedErrorType", ValueText(errType)),
        ("truncatedErrorMessage", ValueText(message)),
      )
  }
}
