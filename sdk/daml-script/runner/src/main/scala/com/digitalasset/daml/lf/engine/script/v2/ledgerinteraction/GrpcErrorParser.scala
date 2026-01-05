// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction

import com.daml.nonempty.NonEmpty
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  SerializationVersion,
}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{Value, ValueCoder}
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.rpc.status.Status

import scala.reflect.ClassTag
import scala.util.Try

object GrpcErrorParser {
  val decodeValue: String => Option[Value] = (s: String) =>
    for {
      bytes <- Try(BaseEncoding.base64().decode(s)).toOption
      value <-
        ValueCoder
          .decodeValue(
            SerializationVersion.VDev,
            ByteString.copyFrom(bytes),
          )
          .toOption
    } yield value

  val decodeNullableValue: String => Option[Option[Value]] =
    decodeNullable(decodeValue)

  val decodeNullableString: String => Option[Option[String]] =
    decodeNullable((s: String) => Some(s))

  def decodeNullable[A](decoder: String => Option[A]): String => Option[Option[A]] = (s: String) =>
    if (s == "NULL") Some(None) else decoder(s).map(Some(_))

  val parseList = (s: String) => s.tail.init.split(", ").toSeq

  // Converts a given SubmitError into a SubmitError. Wraps in an UnknownError if its not what we expect, wraps in a TruncatedError if we're missing resources
  def convertStatusRuntimeException(status: Status): SubmitError = {
    import com.digitalasset.base.error.ErrorResource
    import com.digitalasset.base.error.utils.ErrorDetails._

    val details = from(status.details.map(Any.toJavaProto))
    val message = status.message
    val oErrorInfoDetail = details.collectFirst { case eid: ErrorInfoDetail => eid }
    val errorCode = oErrorInfoDetail.fold("UNKNOWN")(_.errorCodeId)
    val resourceDetails = details.collect { case ResourceInfoDetail(name, res) =>
      (
        ErrorResource
          .fromString(res)
          .getOrElse(
            throw new IllegalArgumentException(s"Unrecognised error resource: \"$res\"")
          ),
        name,
      )
    }

    def classNameOf[A: ClassTag]: String = implicitly[ClassTag[A]].runtimeClass.getSimpleName

    // Builds an appropriate TruncatedError if the given partial function doesn't match
    def caseErr[A <: SubmitError: ClassTag](
        handler: PartialFunction[Seq[(ErrorResource, String)], A]
    ): SubmitError =
      handler
        .lift(resourceDetails)
        .getOrElse(new SubmitError.TruncatedError(classNameOf[A], message))

    def parseParties(parties: String): Set[Party] = {
      if (parties.isBlank) {
        Set.empty
      } else {
        parties.split(",").toSet.map(Party.assertFromString _)
      }
    }

    def parseGlobalKeyWithMaintainers(
        templateId: Identifier,
        globalKeyOpt: Option[Value],
        packageNameOpt: Option[String],
        maintainersOpt: Option[String],
    ): Option[GlobalKeyWithMaintainers] = {
      (globalKeyOpt, packageNameOpt, maintainersOpt) match {
        case (None, None, None) => None
        case (Some(globalKey), Some(packageName), Some(maintainers)) =>
          Some(
            GlobalKeyWithMaintainers.assertBuild(
              templateId,
              globalKey,
              // This GlobalKeyWithMaintainers is only used for rendering errors, and that rendering ignores the hash.
              crypto.Hash.hashPrivateKey("unused-dummy-key-hash"),
              parseParties(maintainers),
              PackageName.assertFromString(packageName),
            )
          )
        case _ =>
          throw new IllegalArgumentException(
            s"""The components of GlobalKeyWithMaintainers should be either all present or all absent:
               |key=$globalKeyOpt, packageName=$packageNameOpt, maintainers=$maintainersOpt""".stripMargin
          )
      }
    }

    errorCode match {
      case "CONTRACT_NOT_FOUND" =>
        caseErr {
          case Seq((ErrorResource.ContractId, cid)) =>
            SubmitError.ContractNotFound(
              NonEmpty(Seq, ContractId.assertFromString(cid)),
              None,
            )
          case Seq((ErrorResource.ContractIds, cids)) =>
            SubmitError.ContractNotFound(
              NonEmpty
                .from(parseList(cids).map(ContractId.assertFromString(_)))
                .getOrElse(
                  throw new IllegalArgumentException(
                    "Got CONTRACT_NOT_FOUND error without any contract ids"
                  )
                ),
              None,
            )
        }
      case "UNRESOLVED_PACKAGE_NAME" =>
        caseErr { case Seq((ErrorResource.PackageName, pkgName)) =>
          SubmitError.UnresolvedPackageName(PackageName.assertFromString(pkgName))
        }
      case "CONTRACT_KEY_NOT_FOUND" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.PackageName, pn),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            val packageName = PackageName.assertFromString(pn)
            SubmitError.ContractKeyNotFound(
              GlobalKey.assertBuild(
                templateId,
                packageName,
                key,
                // This GlobalKeyWithMaintainers is only used for rendering errors, and that rendering ignores the hash.
                crypto.Hash.hashPrivateKey("unused-dummy-key-hash"),
              )
            )
        }
      case "DAML_AUTHORIZATION_ERROR" => SubmitError.AuthorizationError(message)
      case "CONTRACT_NOT_ACTIVE" =>
        caseErr { case Seq((ErrorResource.TemplateId, tid @ _), (ErrorResource.ContractId, cid)) =>
          SubmitError.ContractNotFound(
            NonEmpty(Seq, ContractId.assertFromString(cid)),
            None,
          )
        }
      case "CONTRACT_HASHING_ERROR" =>
        caseErr {
          case Seq(
                (ErrorResource.ContractId, cid),
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractArg, decodeValue.unlift(arg)),
              ) =>
            SubmitError.ContractHashingError(
              ContractId.assertFromString(cid),
              Identifier.assertFromString(tid),
              arg,
              message,
            )
        }
      case "DISCLOSED_CONTRACT_KEY_HASHING_ERROR" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractId, cid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.ContractKeyHash, keyHash),
                (ErrorResource.PackageName, pn),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            val packageName = PackageName.assertFromString(pn)
            SubmitError.DisclosedContractKeyHashingError(
              ContractId.assertFromString(cid),
              GlobalKey.assertBuild(
                templateId,
                packageName,
                key,
                // This GlobalKeyWithMaintainers is only used for rendering errors, and that rendering ignores the hash.
                crypto.Hash.hashPrivateKey("unused-dummy-key-hash"),
              ),
              keyHash,
            )
        }
      case "DUPLICATE_CONTRACT_KEY" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.PackageName, pn),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            val packageName = PackageName.assertFromString(pn)
            SubmitError.DuplicateContractKey(
              Some(
                GlobalKey.assertBuild(
                  templateId,
                  packageName,
                  key,
                  // This GlobalKeyWithMaintainers is only used for rendering errors, and that rendering ignores the hash.
                  crypto.Hash.hashPrivateKey("unused-dummy-key-hash"),
                )
              )
            )
          // TODO[SW] Canton can omit the key, unsure why.
          case Seq() => SubmitError.DuplicateContractKey(None)
        }
      case "LOCAL_VERDICT_LOCKED_KEYS" =>
        caseErr {
          // TODO[MA] Canton does not currently provide the template ids so we
          // can't convert to GlobalKeys.
          // https://github.com/DACH-NY/canton/issues/15071
          case _ => SubmitError.LocalVerdictLockedKeys(Seq())
        }
      case "LOCAL_VERDICT_LOCKED_CONTRACTS" =>
        caseErr {
          // TODO[MA] Canton does not currently provide the template ids so we
          // can't construct the argument to LocalVerdictLockedContracts.
          // https://github.com/DACH-NY/canton/issues/15071
          case _ => SubmitError.LocalVerdictLockedContracts(Seq())
        }
      case "INCONSISTENT_CONTRACT_KEY" =>
        caseErr {

          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.PackageName, pn),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            val packageName = PackageName.assertFromString(pn)
            SubmitError.InconsistentContractKey(
              GlobalKey.assertBuild(
                templateId,
                packageName,
                key,
                // This GlobalKeyWithMaintainers is only used for rendering errors, and that rendering ignores the hash.
                crypto.Hash.hashPrivateKey("unused-dummy-key-hash"),
              )
            )
        }
      case "UNHANDLED_EXCEPTION" =>
        caseErr {
          case Seq(
                (ErrorResource.ExceptionType, ty),
                (ErrorResource.ExceptionValue, decodeValue.unlift(value)),
              ) =>
            SubmitError.UnhandledException(Some((Identifier.assertFromString(ty), value)))
          case Seq() => SubmitError.UnhandledException(None)
        }
      case "INTERPRETATION_USER_ERROR" =>
        caseErr { case Seq((ErrorResource.ExceptionText, excMessage)) =>
          SubmitError.UserError(excMessage)
        }
      case "TEMPLATE_PRECONDITION_VIOLATED" => SubmitError.TemplatePreconditionViolated()
      case "CREATE_EMPTY_CONTRACT_KEY_MAINTAINERS" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractArg, decodeValue.unlift(arg)),
              ) =>
            SubmitError.CreateEmptyContractKeyMaintainers(Identifier.assertFromString(tid), arg)
        }
      case "FETCH_EMPTY_CONTRACT_KEY_MAINTAINERS" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.PackageName, pn),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            val packageName = PackageName.assertFromString(pn)
            SubmitError.FetchEmptyContractKeyMaintainers(
              GlobalKey.assertBuild(
                templateId,
                packageName,
                key,
                // This GlobalKeyWithMaintainers is only used for rendering errors, and that rendering ignores the hash.
                crypto.Hash.hashPrivateKey("unused-dummy-key-hash"),
              )
            )
        }
      case "WRONGLY_TYPED_CONTRACT" =>
        caseErr {
          case Seq(
                (ErrorResource.ContractId, cid),
                (ErrorResource.TemplateId, expectedTid),
                (ErrorResource.TemplateId, actualTid),
              ) =>
            SubmitError.WronglyTypedContract(
              ContractId.assertFromString(cid),
              Identifier.assertFromString(expectedTid),
              Identifier.assertFromString(actualTid),
            )
        }
      case "CONTRACT_DOES_NOT_IMPLEMENT_INTERFACE" =>
        caseErr {
          case Seq(
                (ErrorResource.ContractId, cid),
                (ErrorResource.TemplateId, tid),
                (ErrorResource.InterfaceId, iid),
              ) =>
            SubmitError.ContractDoesNotImplementInterface(
              ContractId.assertFromString(cid),
              Identifier.assertFromString(tid),
              Identifier.assertFromString(iid),
            )
        }
      case "CONTRACT_DOES_NOT_IMPLEMENT_REQUIRING_INTERFACE" =>
        caseErr {
          case Seq(
                (ErrorResource.ContractId, cid),
                (ErrorResource.TemplateId, tid),
                (ErrorResource.InterfaceId, requiredIid),
                (ErrorResource.InterfaceId, requiringIid),
              ) =>
            SubmitError.ContractDoesNotImplementRequiringInterface(
              ContractId.assertFromString(cid),
              Identifier.assertFromString(tid),
              Identifier.assertFromString(requiredIid),
              Identifier.assertFromString(requiringIid),
            )
        }
      case "NON_COMPARABLE_VALUES" => SubmitError.NonComparableValues()
      case "CONTRACT_ID_IN_CONTRACT_KEY" => SubmitError.ContractIdInContractKey()
      case "CONTRACT_ID_COMPARABILITY" =>
        caseErr { case Seq((ErrorResource.ContractId, cid)) =>
          SubmitError.ContractIdComparability(cid)
        }
      case "INTERPRETATION_UPGRADE_ERROR_VALIDATION_FAILED" =>
        val NullableContractKey = ErrorResource.ContractKey.nullable
        val NullablePackageName = ErrorResource.PackageName.nullable
        val NullableParties = ErrorResource.Parties.nullable

        caseErr {
          case Seq(
                (ErrorResource.ContractId, coid),
                (ErrorResource.TemplateId, srcTemplateId),
                (ErrorResource.TemplateId, dstTemplateId),
                (ErrorResource.PackageName, srcPackageName),
                (ErrorResource.PackageName, dstPackageName),
                (ErrorResource.Parties, originalSignatories),
                (ErrorResource.Parties, originalObservers),
                (NullableContractKey, decodeNullableValue.unlift(originalGlobalKey)),
                (NullablePackageName, decodeNullableString.unlift(originalPn)),
                (NullableParties, decodeNullableString.unlift(originalMaintainers)),
                (ErrorResource.Parties, recomputedSignatories),
                (ErrorResource.Parties, recomputedObservers),
                (NullableContractKey, decodeNullableValue.unlift(recomputedGlobalKey)),
                (NullablePackageName, decodeNullableString.unlift(recomputedPn)),
                (NullableParties, decodeNullableString.unlift(recomputedMaintainers)),
              ) =>
            val srcTid = Identifier.assertFromString(srcTemplateId)
            val dstTid = Identifier.assertFromString(dstTemplateId)
            SubmitError.UpgradeError.ValidationFailed(
              ContractId.assertFromString(coid),
              srcTid,
              dstTid,
              PackageName.assertFromString(srcPackageName),
              PackageName.assertFromString(dstPackageName),
              parseParties(originalSignatories),
              parseParties(originalObservers),
              parseGlobalKeyWithMaintainers(
                srcTid,
                originalGlobalKey,
                originalPn,
                originalMaintainers,
              ),
              parseParties(recomputedSignatories),
              parseParties(recomputedObservers),
              parseGlobalKeyWithMaintainers(
                dstTid,
                recomputedGlobalKey,
                recomputedPn,
                recomputedMaintainers,
              ),
              message,
            )
        }
      case "INTERPRETATION_UPGRADE_ERROR_TRANSLATION_FAILED" =>
        val NullableContractId = ErrorResource.ContractId.nullable
        caseErr {
          case Seq(
                (NullableContractId, decodeNullableString.unlift(coidOpt)),
                (ErrorResource.TemplateId, srcTemplateId),
                (ErrorResource.TemplateId, dstTemplateId),
                (ErrorResource.ContractArg, decodeValue.unlift(createArg)),
              ) =>
            SubmitError.UpgradeError.TranslationFailed(
              coidOpt.map(ContractId.assertFromString),
              Identifier.assertFromString(srcTemplateId),
              Identifier.assertFromString(dstTemplateId),
              createArg,
              message,
            )
        }
      case "INTERPRETATION_UPGRADE_ERROR_AUTHENTICATION_FAILED" =>
        caseErr {
          case Seq(
                (ErrorResource.ContractId, coid),
                (ErrorResource.TemplateId, srcTemplateId),
                (ErrorResource.TemplateId, dstTemplateId),
                (ErrorResource.ContractArg, decodeValue.unlift(createArg)),
              ) =>
            SubmitError.UpgradeError.AuthenticationFailed(
              ContractId.assertFromString(coid),
              Identifier.assertFromString(srcTemplateId),
              Identifier.assertFromString(dstTemplateId),
              createArg,
              message,
            )
        }

      case "DAML_FAILURE" => {
        // Fields added automatically by canton, and not by the user
        // Removed in GrpcLedgerClient to be consistent with IDELedgerClient, which will not add these fields
        val cantonFields =
          Seq(
            "commands",
            "definite_answer",
            "tid",
            "category",
            "participant",
            "error_id",
            "exercise_trace",
          )
        val oStatus =
          for {
            errorInfo <- oErrorInfoDetail
            errorId <- errorInfo.metadata.get("error_id")
            category <- errorInfo.metadata.get("category").map(_.toInt)
            metadata = errorInfo.metadata.toMap.removedAll(cantonFields)
            trace = errorInfo.metadata.get("exercise_trace")
            // Drop prefix so we give back the exact message in the throwing daml code
            messageWithoutPrefix <- "^.+?error category \\d+\\): (.*)$".r
              .findFirstMatchIn(message)
              .map(_.group(1))
          } yield SubmitError.FailureStatusError(
            IE.FailureStatus(errorId, category, messageWithoutPrefix, metadata),
            trace,
          )
        oStatus.getOrElse(new SubmitError.TruncatedError("FailureStatusError", message))
      }

      case "INTERPRETATION_CRYPTO_ERROR_MALFORMED_BYTE_ENCODING" =>
        caseErr {
          case Seq(
                (ErrorResource.CryptoValue, value)
              ) =>
            SubmitError.CryptoError.MalformedByteEncoding(value, message)
        }

      case "INTERPRETATION_CRYPTO_ERROR_MALFORMED_KEY" =>
        caseErr {
          case Seq(
                (ErrorResource.CryptoValue, key)
              ) =>
            SubmitError.CryptoError.MalformedKey(key, message)
        }

      case "INTERPRETATION_CRYPTO_ERROR_MALFORMED_SIGNATURE" =>
        caseErr {
          case Seq(
                (ErrorResource.CryptoValue, signature)
              ) =>
            SubmitError.CryptoError.MalformedSignature(signature, message)
        }

      case "INTERPRETATION_DEV_ERROR" =>
        caseErr { case Seq((ErrorResource.DevErrorType, errorType)) =>
          SubmitError.DevError(errorType, message)
        }

      case _ => new SubmitError.UnknownError(message)
    }
  }
}
