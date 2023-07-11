// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.v2.ledgerinteraction

import com.daml.lf.data.Ref._
import com.daml.lf.transaction.{GlobalKey, TransactionVersion}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.ValueCoder
import com.daml.lf.value.ValueCoder.CidDecoder
import com.google.protobuf.ByteString
import io.grpc.StatusRuntimeException
import scala.reflect.ClassTag

object GrpcErrorParser {
  val decodeValue = (s: String) =>
    ValueCoder.decodeValue(CidDecoder, TransactionVersion.VDev, ByteString.copyFromUtf8(s)).toOption

  val parseList = (s: String) => s.tail.init.split(", ").toSeq

  // Converts a given SubmitError into a SubmitError. Wraps in an UnknownError if its not what we expect, wraps in a TruncatedError if we're missing resources
  def convertStatusRuntimeException(s: StatusRuntimeException): SubmitError = {
    import io.grpc.protobuf.StatusProto
    import com.daml.error.utils.ErrorDetails._
    import com.daml.error.ErrorResource

    val grpcStatus = StatusProto.fromThrowable(s)
    val details = from(grpcStatus)
    val message = grpcStatus.getMessage()
    val oErrorInfoDetail = details.collectFirst { case eid: ErrorInfoDetail => eid }
    val errorCode = oErrorInfoDetail.fold("UNKNOWN")(_.errorCodeId)
    val resourceDetails = details.collect { case rd: ResourceInfoDetail => rd }
    val errorResourceFromString = ErrorResource.fromString _

    def classNameOf[A: ClassTag]: String = implicitly[ClassTag[A]].runtimeClass.getSimpleName

    // Builds an appropriate TruncatedError if the given partial function doesn't match
    def caseErr[A <: SubmitError: ClassTag](
        handler: PartialFunction[Seq[(ErrorResource, String)], A]
    ): SubmitError =
      handler
        .lift(
          resourceDetails.collect {
            case ResourceInfoDetail(name, errorResourceFromString.unlift(res)) => (res, name)
          }
        )
        .getOrElse(new SubmitError.TruncatedError(classNameOf[A], message))

    errorCode match {
      case "CONTRACT_NOT_FOUND" =>
        caseErr {
          case Seq((ErrorResource.ContractId, cid)) =>
            SubmitError.ContractNotFound(ContractId.assertFromString(cid))
          // TODO: what should we do here? Currently just taking the first contract id
          case Seq((ErrorResource.ContractIds, cids)) =>
            SubmitError.ContractNotFound(ContractId.assertFromString(parseList(cids).head))
        }
      case "CONTRACT_KEY_NOT_FOUND" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
              ) =>
            SubmitError.ContractKeyNotFound(
              GlobalKey.assertBuild(Identifier.assertFromString(tid), key)
            )
        }
      case "DAML_AUTHORIZATION_ERROR" => SubmitError.AuthorizationError(message)
      case "CONTRACT_NOT_ACTIVE" =>
        caseErr { case Seq((ErrorResource.TemplateId, tid), (ErrorResource.ContractId, cid)) =>
          SubmitError.ContractNotActive(
            Identifier.assertFromString(tid),
            ContractId.assertFromString(cid),
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
            SubmitError.DisclosedContractKeyHashingError(
              ContractId.assertFromString(cid),
              GlobalKey.assertBuild(Identifier.assertFromString(tid), key),
              keyHash,
            )
        }
      case "DUPLICATE_CONTRACT_KEY" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
              ) =>
            SubmitError.DuplicateContractKey(
              GlobalKey.assertBuild(Identifier.assertFromString(tid), key)
            )
        }
      case "INCONSISTENT_CONTRACT_KEY" =>
        caseErr {
          case Seq(
                (ErrorResource.TemplateId, tid),
                (ErrorResource.ContractKey, decodeValue.unlift(key)),
              ) =>
            SubmitError.InconsistentContractKey(
              GlobalKey.assertBuild(Identifier.assertFromString(tid), key)
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
      case "USER_ERROR" =>
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
              ) =>
            SubmitError.FetchEmptyContractKeyMaintainers(
              GlobalKey.assertBuild(Identifier.assertFromString(tid), key)
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
      case "DEV_ERROR" =>
        caseErr { case Seq((ErrorResource.DevErrorType, errorType)) =>
          SubmitError.DevError(errorType, message)
        }

      case _ => new SubmitError.UnknownError(message)
    }
  }
}
