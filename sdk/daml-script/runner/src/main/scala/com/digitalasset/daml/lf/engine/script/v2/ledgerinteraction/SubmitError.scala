// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package v2
package ledgerinteraction

import cats.data.NonEmptyList
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.toApiIdentifier
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, SortedLookupList, Time, Utf8}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.engine.ScriptEngine.ExtendedValue
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
  def toDamlSubmitError(env: ScriptF.Env, legacyAnyContractKey: Boolean): ExtendedValue
}

object SubmitError {
  import ScriptF.Env
  import com.digitalasset.daml.lf.script.converter.Converter._
  import com.digitalasset.daml.lf.engine.script.v2.Converter._

  final case class SubmitErrorConverters(env: ScriptF.Env) {
    def damlScriptErrorIdentifier(s: String) =
      env.scriptIds.damlScriptModule("Daml.Script.Internal.Questions.Submit.Error", s)
    def damlScriptErrorIdentifierUnstable(s: String) =
      env.scriptIds.damlScriptModuleNonStable("Daml.Script.Internal.Questions.Submit.Error", s)

    def damlScriptVariant(
        datatypeName: String,
        variantName: String,
        fields: (String, ExtendedValue)*
    ) =
      damlScriptVariantOrEnum(
        datatypeName,
        variantName,
        (
            (
                dName,
                vName,
            ) =>
              ValueVariant(
                dName,
                vName,
                record(
                  damlScriptErrorIdentifier(datatypeName + "." + variantName),
                  fields: _*
                ),
              ),
        ),
        fields: _*
      )

    def damlScriptEnum(
        datatypeName: String,
        variantName: String,
    ) = damlScriptVariantOrEnum(datatypeName, variantName, ValueEnum(_, _))

    def damlScriptVariantOrEnum(
        datatypeName: String,
        variantName: String,
        buildLegacy: (Option[Identifier], Name) => ExtendedValue,
        fields: (String, ExtendedValue)*
    ) =
      env.scriptIds.scriptEra match {
        case ScriptIds.ScriptEra.Legacy(_) =>
          throw new IllegalArgumentException("Unsupported daml-script era: Legacy")
        case ScriptIds.ScriptEra.NonStable(_) =>
          buildLegacy(
            Some(damlScriptErrorIdentifier(datatypeName)),
            Name.assertFromString(variantName),
          )
        case ScriptIds.ScriptEra.Stable(_, _) =>
          // In Stable, the "Variant" is now a wrapper around a "TaggedRecord", which holds the fields in a
          // Map from Text to "LedgerValue" (i.e. the original type, made opaque)
          record(
            damlScriptErrorIdentifier("Any" + datatypeName),
            (
              "unpack",
              record(
                damlScriptErrorIdentifier("TaggedRecord"),
                (
                  "tgTag",
                  ValueText(variantName),
                ),
                (
                  "tgData",
                  ValueGenMap(ImmArray.from(fields.sortBy(_._1)(Utf8.Ordering).map { case (k, v) =>
                    ValueText(k) -> v
                  })),
                ),
              ),
            ),
          )
      }
    // For types that are unstable regardless of script era
    def damlScriptVariantUnstable(
        datatypeName: String,
        variantName: String,
        fields: (String, ExtendedValue)*
    ) =
      env.scriptIds.scriptEra match {
        case ScriptIds.ScriptEra.Legacy(_) =>
          throw new IllegalArgumentException("Unsupported daml-script era: Legacy")
        case ScriptIds.ScriptEra.NonStable(_) | ScriptIds.ScriptEra.Stable(_, _) =>
          ValueVariant(
            Some(damlScriptErrorIdentifierUnstable(datatypeName)),
            Name.assertFromString(variantName),
            record(
              damlScriptErrorIdentifierUnstable(datatypeName + "." + variantName),
              fields: _*
            ),
          )
      }

    def damlScriptError(name: String, originalMessage: String, fields: (String, ExtendedValue)*) =
      // Handling for mismatching runner and daml-script library versions, by constructing errors by name, not by rank
      env.scriptIds.scriptEra match {
        case ScriptIds.ScriptEra.Legacy(_) =>
          throw new IllegalArgumentException("Unsupported daml-script era: Legacy")
        case ScriptIds.ScriptEra.NonStable(_)
            if env.doesVariantConstructorExist(
              damlScriptErrorIdentifier("SubmitError"),
              Name.assertFromString(name),
            ) =>
          damlScriptVariant("SubmitError", name, fields: _*)
        case ScriptIds.ScriptEra.NonStable(_) =>
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
        case ScriptIds.ScriptEra.Stable(_, _) =>
          damlScriptVariant(
            "SubmitError",
            name,
            (("originalMessage", ValueText(originalMessage)) +: fields): _*
          )
      }
  }

  def globalKeyToAnyContractKey(
      env: Env,
      legacyAnyContractKey: Boolean,
      key: GlobalKey,
  ): ExtendedValue = {
    val ty = env.lookupKeyTy(key.templateId).toOption.get
    val enrichedKey = env.enricher
      .enrichContractKey(key.templateId, key.key)(env.traceContext)
      .consume(lookupHandler())
      .toOption
      .get
    fromAnyContractKey(
      env.scriptIds,
      AnyContractKey(key.templateId, ty, enrichedKey),
      legacyAnyContractKey,
    )
  }

  def fromNonEmptySet[A](set: NonEmptyList[A], conv: A => ExtendedValue): ExtendedValue = {
    val converted: Seq[ExtendedValue] = set.toList.map(conv)
    record(
      StablePackagesV2.NonEmpty,
      ("hd", converted.head),
      ("tl", ValueList(converted.tail.to(FrontStack))),
    )
  }

  final case class ContractNotFound(
      cids: NonEmptyList[ContractId],
      additionalDebuggingInfo: Option[ContractNotFound.AdditionalInfo],
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractNotFound",
        originalMessage,
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

  final case class UnsupportedContractId(cid: ContractId, originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnsupportedContractId",
        originalMessage,
        ("unknownContractId", ValueText(cid.coid)),
      )
  }

  final case class UnresolvedPackageName(packageName: PackageName, originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnresolvedPackageName",
        originalMessage,
        ("packageName", ValueText(packageName)),
      )
  }

  final case class EffectfulRollback(message: String) extends SubmitError {
    def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "EffectfulRollbackError",
        message,
        (
          "effectfulRollbackErrorMsg",
          ValueText(message),
        ),
      )
  }

  object ContractNotFound {

    sealed abstract class AdditionalInfo {
      def toValue(env: Env): ExtendedValue
    }

    object AdditionalInfo {
      final case class NotFound() extends AdditionalInfo {
        override def toValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariantUnstable(
            "ContractNotFoundAdditionalInfo",
            "NotFound",
          )
      }

      final case class NotActive(
          cid: ContractId,
          tid: Identifier,
      ) extends AdditionalInfo {
        override def toValue(env: Env) =
          SubmitErrorConverters(env).damlScriptVariantUnstable(
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
          SubmitErrorConverters(env).damlScriptVariantUnstable(
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
          SubmitErrorConverters(env).damlScriptVariantUnstable(
            "ContractNotFoundAdditionalInfo",
            "NotVisible",
            (
              "additionalInfoCid",
              fromAnyContractId(env.scriptIds, toApiIdentifier(tid), cid),
            ),
            (
              "actAs",
              ValueList(actAs.toList.map(ValueParty.apply).to(FrontStack)),
            ),
            (
              "readAs",
              ValueList(readAs.toList.map(ValueParty.apply).to(FrontStack)),
            ),
            (
              "observers",
              ValueList(observers.toList.map(ValueParty.apply).to(FrontStack)),
            ),
          )
      }
    }
  }

  final case class ContractKeyNotFound(key: GlobalKey, originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractKeyNotFound",
        originalMessage,
        ("contractKey", globalKeyToAnyContractKey(env, legacyAnyContractKey, key)),
      )
  }

  final case class AuthorizationError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "AuthorizationError",
        message,
        ("authorizationErrorMessage", ValueText(message)),
      )
  }

  final case class ContractHashingError(
      coid: ContractId,
      dstTemplateId: TypeConId,
      createArg: Value,
      message: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
      SubmitErrorConverters(env).damlScriptError(
        "ContractHashingError",
        message,
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
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "DisclosedContractKeyHashingError",
        originalMessage,
        (
          "contractId",
          fromAnyContractId(env.scriptIds, toApiIdentifier(key.templateId), contractId),
        ),
        ("expectedKey", globalKeyToAnyContractKey(env, legacyAnyContractKey, key)),
        ("givenKeyHash", ValueText(givenKeyHash)),
      )
  }

  final case class DuplicateContractKey(oKey: Option[GlobalKey], originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "DuplicateContractKey",
        originalMessage,
        (
          "duplicateContractKey",
          ValueOptional(oKey.map(globalKeyToAnyContractKey(env, legacyAnyContractKey, _))),
        ),
      )
  }

  final case class InconsistentContractKey(key: GlobalKey, originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "InconsistentContractKey",
        originalMessage,
        ("contractKey", globalKeyToAnyContractKey(env, legacyAnyContractKey, key)),
      )
  }

  final case class UnhandledException(exc: Option[(Identifier, Value)], originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
      val anyException = exc.map { case (ty, value) =>
        fromAnyException(
          ty,
          env.enricher
            .enrichValue(Ast.TTyCon(ty), value)(env.traceContext)
            .consume(lookupHandler())
            .toOption
            .get,
        )
      }
      SubmitErrorConverters(env).damlScriptError(
        "UnhandledException",
        originalMessage,
        ("exc", ValueOptional(anyException)),
      )
    }
  }

  final case class UserError(message: String, originalMessage: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "UserError",
        originalMessage,
        ("userErrorMessage", ValueText(message)),
      )
  }

  final case class TemplatePreconditionViolated(originalMessage: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "TemplatePreconditionViolated",
        originalMessage,
      )
  }

  final case class CreateEmptyContractKeyMaintainers(
      templateId: Identifier,
      templateArg: Value,
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "CreateEmptyContractKeyMaintainers",
        originalMessage,
        (
          "invalidTemplate", {
            val enrichedArg =
              env.enricher
                .enrichContract(templateId, templateArg)(env.traceContext)
                .consume(lookupHandler())
                .toOption
                .get
            fromAnyTemplate(templateId, enrichedArg)
          },
        ),
      )
  }

  final case class FetchEmptyContractKeyMaintainers(key: GlobalKey, originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "FetchEmptyContractKeyMaintainers",
        originalMessage,
        ("failedTemplateKey", globalKeyToAnyContractKey(env, legacyAnyContractKey, key)),
      )
  }

  final case class WronglyTypedContract(
      contractId: ContractId,
      expectedTemplateId: Identifier,
      actualTemplateId: Identifier,
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "WronglyTypedContract",
        originalMessage,
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
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractDoesNotImplementInterface",
        originalMessage,
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
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractDoesNotImplementInterface",
        originalMessage,
        ("contractId", fromAnyContractId(env.scriptIds, toApiIdentifier(templateId), contractId)),
        ("templateId", fromTemplateTypeRep(toApiIdentifier(templateId))),
        ("requiredInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiredInterfaceId))),
        ("requiringInterfaceId", fromTemplateTypeRep(toApiIdentifier(requiringInterfaceId))),
      )
  }

  final case class NonComparableValues(originalMessage: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "NonComparableValues",
        originalMessage,
      )
  }

  final case class ContractIdInContractKey(originalMessage: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdInContractKey",
        originalMessage,
      )
  }

  final case class ContractIdComparability(contractId: String, originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ContractIdComparability",
        originalMessage,
        ("globalExistingContractId", ValueText(contractId)),
      )
  }

  final case class ValueNesting(limit: Int, originalMessage: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ValueNesting",
        originalMessage,
        ("limit", ValueInt64(limit.toLong)),
      )
  }

  final case class MalformedText(message: String, originalMessage: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "MalformedText",
        originalMessage,
        ("malformedTextMessage", ValueText(message)),
      )
  }

  final case class LocalVerdictLockedContracts(
      cids: Seq[(Identifier, ContractId)],
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedContracts",
        originalMessage,
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

  final case class LocalVerdictLockedKeys(keys: Seq[GlobalKey], originalMessage: String)
      extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "LocalVerdictLockedKeys",
        originalMessage,
        (
          "localVerdictLockedKeys",
          ValueList(
            keys.map(globalKeyToAnyContractKey(env, legacyAnyContractKey, _)).to(FrontStack)
          ),
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
        originalNonSignatoryStakeholders: Set[Party],
        originalOptKey: Option[GlobalKeyWithMaintainers],
        recomputedSignatories: Set[Party],
        recomputedNonSignatoryStakeholders: Set[Party],
        recomputedOptKey: Option[GlobalKeyWithMaintainers],
        message: String,
    ) extends SubmitError {
      override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
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
              ValueList(originalSignatories.toList.map(ValueParty.apply).to(FrontStack)),
            ),
            (
              "originalNonSignatoryStakeholders",
              ValueList(
                originalNonSignatoryStakeholders.toList.map(ValueParty.apply).to(FrontStack)
              ),
            ),
            (
              "originalKeyOpt",
              ValueOptional(originalOptKey.map(key => {
                val globalKey = globalKeyToAnyContractKey(env, legacyAnyContractKey, key.globalKey)
                val maintainers =
                  ValueList(key.maintainers.toList.map(ValueParty.apply).to(FrontStack))
                makeTuple(globalKey, maintainers)
              })),
            ),
            (
              "recomputedSignatories",
              ValueList(recomputedSignatories.toList.map(ValueParty.apply).to(FrontStack)),
            ),
            (
              "recomputedNonSignatoryStakeholders",
              ValueList(
                recomputedNonSignatoryStakeholders.toList.map(ValueParty.apply).to(FrontStack)
              ),
            ),
            (
              "recomputedKeyOpt",
              ValueOptional(recomputedOptKey.map(key => {
                val globalKey = globalKeyToAnyContractKey(env, legacyAnyContractKey, key.globalKey)
                val maintainers =
                  ValueList(key.maintainers.toList.map(ValueParty.apply).to(FrontStack))
                makeTuple(globalKey, maintainers)
              })),
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          message,
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
      override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
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
                  env.enricher
                    .enrichContract(srcTemplateId, createArg)(env.traceContext)
                    .consume(lookupHandler())
                    .toOption
                    .get
                fromAnyTemplate(srcTemplateId, enrichedArg)
              },
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          message,
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
      override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
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
                  env.enricher
                    .enrichContract(srcTemplateId, createArg)(env.traceContext)
                    .consume(lookupHandler())
                    .toOption
                    .get
                fromAnyTemplate(srcTemplateId, enrichedArg)
              },
            ),
          )
        SubmitErrorConverters(env).damlScriptError(
          "UpgradeError",
          message,
          ("errorType", upgradeErrorType),
          ("errorMessage", ValueText(message)),
        )
      }
    }
  }

  final case class FailureStatusError(
      failureStatus: IE.FailureStatus,
      exerciseTrace: Option[String],
      originalMessage: String,
  ) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "FailureStatusError",
        originalMessage,
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
                SortedLookupList.from(failureStatus.metadata.view.mapValues(ValueText(_)).toMap)
              ),
            ),
          ),
        ),
      )
  }

  object CryptoError {
    final case class MalformedByteEncoding(
        value: String,
        message: String,
    ) extends SubmitError {
      override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
        val errorType =
          damlScriptCryptoErrorType(env, "MalformedByteEncoding", "value" -> ValueText(value))

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          message,
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", ValueText(message)),
        )
      }
    }

    final case class MalformedKey(key: String, message: String) extends SubmitError {
      override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
        val errorType = damlScriptCryptoErrorType(env, "MalformedKey", "keyValue" -> ValueText(key))

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          message,
          ("cryptoErrorType", errorType),
          ("cryptoErrorMessage", ValueText(message)),
        )
      }
    }

    final case class MalformedSignature(
        signature: String,
        message: String,
    ) extends SubmitError {
      override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
        val errorType = damlScriptCryptoErrorType(
          env,
          "MalformedSignature",
          "signatureValue" -> ValueText(signature),
        )

        SubmitErrorConverters(env).damlScriptError(
          "CryptoError",
          message,
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

  final case class ExternalCallError(
      errorType: ExternalCallError.ErrorType,
      extensionId: String,
      functionId: String,
      message: String,
      originalMessage: String,
  ) extends SubmitError {
    // This code needs to be kept in sync with daml-script#Error.daml
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "ExternalCallError",
        originalMessage,
        (
          "externalCallErrorType",
          SubmitErrorConverters(env).damlScriptEnum("ExternalCallErrorType", errorType.name),
        ),
        ("extensionId", ValueText(extensionId)),
        ("functionId", ValueText(functionId)),
        ("externalCallErrorMessage", ValueText(message)),
      )
  }

  object ExternalCallError {
    sealed abstract class ErrorType(val name: String)
    object ErrorType {
      case object PreparationFailed extends ErrorType("PreparationFailed")
      case object ExecutionFailed extends ErrorType("ExecutionFailed")
      case object InvalidOutput extends ErrorType("InvalidOutput")
    }
  }

  final case class DevError(errorType: String, message: String) extends SubmitError {
    // This code needs to be kept in sync with daml-script#Error.daml
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue = {
      SubmitErrorConverters(env).damlScriptError(
        "DevError",
        message,
        (
          "devErrorType",
          SubmitErrorConverters(env).damlScriptEnum(
            "DevErrorType",
            errorType match {
              case "ChoiceGuardFailed" => "ChoiceGuardFailed"
              case _ => "UnknownNewFeature"
            },
          ),
        ),
        ("devErrorMessage", ValueText(message)),
      )
    }
  }

  final case class UnknownError(message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "UnknownError",
        message,
        ("unknownErrorMessage", ValueText(message)),
      )
  }

  final case class TruncatedError(errType: String, message: String) extends SubmitError {
    override def toDamlSubmitError(env: Env, legacyAnyContractKey: Boolean): ExtendedValue =
      SubmitErrorConverters(env).damlScriptError(
        "TruncatedError",
        message,
        ("truncatedErrorType", ValueText(errType)),
        ("truncatedErrorMessage", ValueText(message)),
      )
  }
}
