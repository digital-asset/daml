// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.v2.ledgerinteraction

import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref._
import com.daml.lf.data.assertRight
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.transaction.{GlobalKey, TransactionVersion}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.ValueCoder
import com.daml.lf.value.ValueCoder.CidDecoder
import com.daml.nonempty.NonEmpty
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException

import scala.reflect.ClassTag
import scala.util.Try

object GrpcErrorParser {
  private val parsers = LanguageMajorVersion.All.map(v => v -> new GrpcErrorParser(v)).toMap

  def apply(majorLanguageVersion: LanguageMajorVersion): GrpcErrorParser =
    parsers(majorLanguageVersion)
}

class GrpcErrorParser(majorVersion: LanguageMajorVersion) {
  val submitErrors = new SubmitErrors(majorVersion)

  val decodeValue = (s: String) =>
    for {
      bytes <- Try(BaseEncoding.base64().decode(s)).toOption
      value <-
        ValueCoder
          .decodeValue(
            CidDecoder,
            TransactionVersion.VDev,
            ByteString.copyFrom(bytes),
          )
          .toOption
    } yield value

  val parseList = (s: String) => s.tail.init.split(", ").toSeq

  // Converts a given SubmitError into a SubmitError. Wraps in an UnknownError if its not what we expect, wraps in a TruncatedError if we're missing resources
  def convertStatusRuntimeException(
      s: StatusRuntimeException,
      keyPackageNameLookup: PackageId => Either[String, KeyPackageName],
  ): SubmitError = {
    import io.grpc.protobuf.StatusProto
    import com.daml.error.utils.ErrorDetails._
    import com.daml.error.ErrorResource

    val grpcStatus = StatusProto.fromThrowable(s)
    val details = from(grpcStatus)
    val message = grpcStatus.getMessage()
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

    def assertKeyPackageName(packageId: PackageId): KeyPackageName =
      assertRight(keyPackageNameLookup(packageId))

    def classNameOf[A: ClassTag]: String = implicitly[ClassTag[A]].runtimeClass.getSimpleName

    // Builds an appropriate TruncatedError if the given partial function doesn't match
    def caseErr[A <: SubmitError: ClassTag](
        handler: PartialFunction[Seq[(ErrorResource, String)], A]
    ): SubmitError =
      handler
        .lift(resourceDetails)
        .getOrElse(new submitErrors.TruncatedError(classNameOf[A], message))

    errorCode match {
      case "CONTRACT_NOT_FOUND" =>
        caseErr {
          case Seq((ErrorResource.ContractId, cid)) =>
            submitErrors.ContractNotFound(
              NonEmpty(Seq, ContractId.assertFromString(cid)),
              None,
            )
          case Seq((ErrorResource.ContractIds, cids)) =>
            submitErrors.ContractNotFound(
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
      case "CONTRACT_KEY_NOT_FOUND" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.ContractKeyNotFound(
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId))
            )

          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.SharedKey, _),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.ContractKeyNotFound(
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId))
            )
        }
      case "DAML_AUTHORIZATION_ERROR" => submitErrors.AuthorizationError(message)
      case "CONTRACT_NOT_ACTIVE" =>
        caseErr { case Seq((ErrorResource.TemplateId, tid @ _), (ErrorResource.ContractId, cid)) =>
          submitErrors.ContractNotFound(
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
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.DisclosedContractKeyHashingError(
              ContractId.assertFromString(cid),
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId)),
              keyHash,
            )

          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractId, cid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.SharedKey, _),
                (ErrorResource.ContractKeyHash, keyHash),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.DisclosedContractKeyHashingError(
              ContractId.assertFromString(cid),
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId)),
              keyHash,
            )
        }
      case "DUPLICATE_CONTRACT_KEY" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.DuplicateContractKey(
              Some(
                GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId))
              )
            )
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.SharedKey, _),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.DuplicateContractKey(
              Some(
                GlobalKey.assertBuild(
                  templateId,
                  key,
                  assertKeyPackageName(templateId.packageId),
                )
              )
            )
          // TODO[SW] Canton can omit the key, unsure why.
          case Seq() => submitErrors.DuplicateContractKey(None)
        }
      case "LOCAL_VERDICT_LOCKED_KEYS" =>
        caseErr {
          // TODO[MA] Canton does not currently provide the template ids so we
          // can't convert to GlobalKeys.
          // https://github.com/DACH-NY/canton/issues/15071
          case _ => submitErrors.LocalVerdictLockedKeys(Seq())
        }
      case "LOCAL_VERDICT_LOCKED_CONTRACTS" =>
        caseErr {
          // TODO[MA] Canton does not currently provide the template ids so we
          // can't construct the argument to LocalVerdictLockedContracts.
          // https://github.com/DACH-NY/canton/issues/15071
          case _ => submitErrors.LocalVerdictLockedContracts(Seq())
        }
      case "INCONSISTENT_CONTRACT_KEY" =>
        caseErr {

          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.InconsistentContractKey(
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId))
            )

          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.SharedKey, _),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.InconsistentContractKey(
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId))
            )
        }
      case "UNHANDLED_EXCEPTION" =>
        caseErr {
          case Seq(
                (ErrorResource.ExceptionType, ty),
                (ErrorResource.ExceptionValue, decodeValue.unlift(value)),
              ) =>
            submitErrors.UnhandledException(Some((Identifier.assertFromString(ty), value)))
          case Seq() => submitErrors.UnhandledException(None)
        }
      case "INTERPRETATION_USER_ERROR" =>
        caseErr { case Seq((ErrorResource.ExceptionText, excMessage)) =>
          submitErrors.UserError(excMessage)
        }
      case "TEMPLATE_PRECONDITION_VIOLATED" => submitErrors.TemplatePreconditionViolated()
      case "CREATE_EMPTY_CONTRACT_KEY_MAINTAINERS" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractArg, decodeValue.unlift(arg)),
              ) =>
            submitErrors.CreateEmptyContractKeyMaintainers(Identifier.assertFromString(tid), arg)
        }
      case "FETCH_EMPTY_CONTRACT_KEY_MAINTAINERS" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.FetchEmptyContractKeyMaintainers(
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId))
            )

          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
                (ErrorResource.SharedKey, _),
              ) =>
            val templateId = Identifier.assertFromString(tid)
            submitErrors.FetchEmptyContractKeyMaintainers(
              GlobalKey.assertBuild(templateId, key, assertKeyPackageName(templateId.packageId))
            )
        }
      case "WRONGLY_TYPED_CONTRACT" =>
        caseErr {
          case Seq(
                (ErrorResource.ContractId, cid),
                (ErrorResource.TemplateId, expectedTid),
                (ErrorResource.TemplateId, actualTid),
              ) =>
            submitErrors.WronglyTypedContract(
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
            submitErrors.ContractDoesNotImplementInterface(
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
            submitErrors.ContractDoesNotImplementRequiringInterface(
              ContractId.assertFromString(cid),
              Identifier.assertFromString(tid),
              Identifier.assertFromString(requiredIid),
              Identifier.assertFromString(requiringIid),
            )
        }
      case "NON_COMPARABLE_VALUES" => submitErrors.NonComparableValues()
      case "CONTRACT_ID_IN_CONTRACT_KEY" => submitErrors.ContractIdInContractKey()
      case "CONTRACT_ID_COMPARABILITY" =>
        caseErr { case Seq((ErrorResource.ContractId, cid)) =>
          submitErrors.ContractIdComparability(cid)
        }
      case "INTERPRETATION_DEV_ERROR" =>
        caseErr { case Seq((ErrorResource.DevErrorType, errorType)) =>
          submitErrors.DevError(errorType, message)
        }

      case _ => new submitErrors.UnknownError(message)
    }
  }
}
