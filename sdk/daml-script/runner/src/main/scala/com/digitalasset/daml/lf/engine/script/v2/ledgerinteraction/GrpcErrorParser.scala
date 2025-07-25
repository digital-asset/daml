// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  TransactionVersion,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.ValueCoder
import com.daml.nonempty.NonEmpty
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import com.google.protobuf.any.Any

import scala.reflect.ClassTag
import scala.util.Try

object GrpcErrorParser {
  val decodeValue = (s: String) =>
    for {
      bytes <- Try(BaseEncoding.base64().decode(s)).toOption
      value <-
        ValueCoder
          .decodeValue(
            TransactionVersion.VDev,
            ByteString.copyFrom(bytes),
          )
          .toOption
    } yield value

  val parseList = (s: String) => s.tail.init.split(", ").toSeq

  // Converts a given SubmitError into a SubmitError. Wraps in an UnknownError if its not what we expect, wraps in a TruncatedError if we're missing resources
  def convertStatusRuntimeException(status: Status): SubmitError = {
    import com.digitalasset.base.error.utils.ErrorDetails._
    import com.digitalasset.base.error.ErrorResource

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
              GlobalKey.assertBuild(templateId, key, packageName)
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
              GlobalKey.assertBuild(templateId, key, packageName),
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
              Some(GlobalKey.assertBuild(templateId, key, packageName))
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
              GlobalKey.assertBuild(templateId, key, packageName)
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
              GlobalKey.assertBuild(templateId, key, packageName)
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
        caseErr {
          case Seq(
                (ErrorResource.ContractId, coid),
                (ErrorResource.TemplateId, srcTemplateId),
                (ErrorResource.TemplateId, dstTemplateId),
                (ErrorResource.Parties, signatories),
                (ErrorResource.Parties, observers),
              ) =>
            SubmitError.UpgradeError.ValidationFailed(
              ContractId.assertFromString(coid),
              Identifier.assertFromString(srcTemplateId),
              Identifier.assertFromString(dstTemplateId),
              parseParties(signatories),
              parseParties(observers),
              None,
              message,
            )
          case Seq(
                (ErrorResource.ContractId, coid),
                (ErrorResource.TemplateId, srcTemplateId),
                (ErrorResource.TemplateId, dstTemplateId),
                (ErrorResource.Parties, signatories),
                (ErrorResource.Parties, observers),
                (ErrorResource.ContractKey, decodeValue.unlift(globalKey)),
                (ErrorResource.PackageName, pn),
                (ErrorResource.Parties, maintainers),
              ) =>
            val dstTid = Identifier.assertFromString(dstTemplateId)
            val packageName = PackageName.assertFromString(pn)
            SubmitError.UpgradeError.ValidationFailed(
              ContractId.assertFromString(coid),
              Identifier.assertFromString(srcTemplateId),
              dstTid,
              parseParties(signatories),
              parseParties(observers),
              Some(
                GlobalKeyWithMaintainers.assertBuild(
                  dstTid,
                  globalKey,
                  parseParties(maintainers),
                  packageName,
                )
              ),
              message,
            )
        }
      case "INTERPRETATION_UPGRADE_ERROR_DOWNGRADE_DROP_DEFINED_FIELD" =>
        caseErr {
          case Seq(
                (ErrorResource.ExpectedType, expectedType),
                (ErrorResource.FieldIndex, fieldIndex),
              ) =>
            SubmitError.UpgradeError.DowngradeDropDefinedField(
              expectedType,
              fieldIndex.toLong,
              message,
            )
        }

      case "INTERPRETATION_UPGRADE_ERROR_DOWNGRADE_FAILED" =>
        caseErr {
          case Seq(
                (ErrorResource.ExpectedType, expectedType)
              ) =>
            SubmitError.UpgradeError.DowngradeFailed(expectedType, message)
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

      case "INTERPRETATION_CRYPTO_ERROR_MALFORMED_CONTRACT_ID" =>
        caseErr {
          case Seq(
                (ErrorResource.CryptoValue, value)
              ) =>
            SubmitError.CryptoError.MalformedContractId(value, message)
        }

      case "INTERPRETATION_DEV_ERROR" =>
        caseErr { case Seq((ErrorResource.DevErrorType, errorType)) =>
          SubmitError.DevError(errorType, message)
        }

      case _ => new SubmitError.UnknownError(message)
    }
  }
}
