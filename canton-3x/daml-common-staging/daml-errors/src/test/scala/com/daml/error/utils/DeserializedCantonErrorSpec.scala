// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.*
import com.daml.error.utils.ErrorDetails.ErrorDetail
import com.google.protobuf.any.Any
import com.google.protobuf.any.Any.toJavaProto
import com.google.rpc.error_details.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import com.google.rpc.status.Status as RpcStatus
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues, OptionValues}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

class DeserializedCantonErrorSpec
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with OptionValues
    with ScalaCheckPropertyChecks {
  behavior of DeserializedCantonError.getClass.getSimpleName

  it should s"correctly deserialize a gRPC status code to a ${BaseError.getClass.getSimpleName}" in
    forAll(errorCategoriesTable) { errorCategory =>
      forAll(propertyMapTable) { propertyMap =>
        forAll(correlationIdTable) { correlationId =>
          forAll(traceIdTable) { traceId =>
            val fromGrpc: RpcStatus => DeserializedCantonError =
              DeserializedCantonError.fromGrpcStatus(_).value
            val toGrpc: DeserializedCantonError => RpcStatus = _.toRpcStatusWithForwardedRequestId

            val someGrpcStatus =
              createGrpcStatus(errorCategory, propertyMap, correlationId, traceId)

            checkEquivalence(someGrpcStatus, toGrpc(fromGrpc(someGrpcStatus)))
          }
        }
      }
    }

  it should "fallback to the original cause if error message format is not recognized" in {
    implicit val contextErrorLogger: ContextualizedErrorLogger = NoLogging

    val nonStandardMessage = "!!!Some non-standard message"
    val error = BenignError.Reject("nvm").rpcStatus().copy(message = nonStandardMessage)
    val deserializedError = DeserializedCantonError.fromGrpcStatus(error).value

    deserializedError.cause shouldBe nonStandardMessage
  }

  private val errorGrpcStatus =
    createGrpcStatus(ErrorCategory.TransientServerFailure, Map("1" -> "2"), Some("c"), Some("t"))

  it should "return a Left on missing category information in error metadata" in {
    val errInfo = errorGrpcStatus.details.find(_ is ErrorInfo).value.unpack[ErrorInfo]
    val modifiedErrInfo = errInfo.copy(metadata = errInfo.metadata.removed("category"))
    val modifiedErrorGrpcStatus = errorGrpcStatus.copy(details =
      errorGrpcStatus.details.filterNot(_ is ErrorInfo) :+ Any.pack(modifiedErrInfo)
    )

    DeserializedCantonError.fromGrpcStatus(modifiedErrorGrpcStatus).left.value should include(
      "category key not found in error metadata"
    )
  }

  it should s"return a Left on invalid number of ${classOf[ErrorInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[ErrorInfo](
      "exactly one",
      Map(0 -> false, 1 -> true, 2 -> false),
    )
  }

  it should s"return a Left on invalid number of ${classOf[RequestInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[RequestInfo]("at most one", Map(0 -> true, 1 -> true, 2 -> false))
  }

  it should s"return a Left on invalid number of ${classOf[RetryInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[RetryInfo]("at most one", Map(0 -> true, 1 -> true, 2 -> false))
  }

  it should s"allow any number of ${classOf[ResourceInfo].getSimpleName} in the gRPC status" in {
    testErrorDetails[ResourceInfo]("doesn't matter", Map(0 -> true, 1 -> true, 2 -> true))
  }

  private def testErrorDetails[T <: GeneratedMessage](
      arityMsg: String,
      allowedNumbers: Map[Int, Boolean],
  )(implicit
      expectedTypeCompanion: GeneratedMessageCompanion[T]
  ): Assertion = {
    allowedNumbers.foreach { case (times, expectPass) =>
      val errorDetails = errorGrpcStatus.details
      val errDetail = errorDetails.find(_ is expectedTypeCompanion).value
      val modifiedStatus = errorGrpcStatus.copy(details =
        errorDetails.filterNot(_ is expectedTypeCompanion) ++ (1 to times).map(_ => errDetail)
      )

      val actual = DeserializedCantonError.fromGrpcStatus(modifiedStatus)

      if (expectPass)
        actual.isRight shouldBe true
      else
        actual shouldBe Left(
          s"Could not extract error detail. Expected $arityMsg ${expectedTypeCompanion.scalaDescriptor.fullName} in status details, but got $times"
        )
    }
    succeed
  }

  private def createGrpcStatus(
      errorCategory: ErrorCategory,
      propertyMap: Map[String, String],
      correlationId: Option[String],
      traceId: Option[String],
  ) = {
    implicit val contextErrorLogger: NoLogging = new NoLogging(propertyMap, correlationId, traceId)
    implicit val errorCode: ErrorCode =
      new ErrorCode("SOME_ERROR_CODE_ID", errorCategory)(ErrorClass(List.empty)) {}
    {
      new DamlErrorWithDefiniteAnswer(
        cause = "Some cause",
        throwableO = Some(new RuntimeException("oups")),
        definiteAnswer = true,
        extraContext = Map("key" -> "val"),
      ) {
        override def resources: Seq[(ErrorResource, String)] =
          super.resources :+ (ErrorResource.CommandId -> "some-cmd-id")
      }
    }.rpcStatus()
  }

  private def propertyMapTable = Table(
    "property map",
    Map("p1" -> "k1", "p2" -> "k2"),
    Map("p1" -> "k1"),
    Map.empty[String, String],
  )

  private def correlationIdTable = Table(
    "correlation id",
    Some("corr-id"),
    None,
  )

  private def traceIdTable = Table("trace id", Some("trace-id"), None)

  private def errorCategoriesTable = {
    val grpcAwareErrorCats = ErrorCategory.all.filter(_.grpcCode.nonEmpty)
    Table(
      "error category",
      grpcAwareErrorCats: _*
    )
  }

  // Manual equality check for rpc statuses since encoding of maps (error details)
  // in ByteString is non-deterministic
  private def checkEquivalence(
      rpcStatusOriginal: RpcStatus,
      rpcStatusFromReconstructedError: RpcStatus,
  ): Assertion = {
    def refine(details: Seq[com.google.protobuf.any.Any]): Seq[ErrorDetail] =
      ErrorDetails.from(details.map(toJavaProto))

    rpcStatusFromReconstructedError.code shouldBe rpcStatusOriginal.code
    rpcStatusFromReconstructedError.message shouldBe rpcStatusOriginal.message
    val recontructedErrorDetails = refine(rpcStatusFromReconstructedError.details)
    val originalErrorDetails = refine(rpcStatusOriginal.details)
    recontructedErrorDetails shouldBe originalErrorDetails
  }
}
