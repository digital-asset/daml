// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.{BaseError, ErrorCode}
import com.google.protobuf
import com.google.rpc.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

object ErrorDetails {

  sealed trait ErrorDetail extends Product with Serializable

  final case class ResourceInfoDetail(name: String, typ: String) extends ErrorDetail
  final case class ErrorInfoDetail(errorCodeId: String, metadata: Map[String, String])
      extends ErrorDetail
  final case class RetryInfoDetail(duration: Duration) extends ErrorDetail
  final case class RequestInfoDetail(requestId: String) extends ErrorDetail

  def from(e: StatusRuntimeException): Seq[ErrorDetail] =
    from(StatusProto.fromThrowable(e).getDetailsList.asScala.toSeq)

  def from(anys: Seq[protobuf.Any]): Seq[ErrorDetail] = anys.toList.map {
    case any if any.is(classOf[ResourceInfo]) =>
      val v = any.unpack(classOf[ResourceInfo])
      ResourceInfoDetail(v.getResourceType, v.getResourceName)

    case any if any.is(classOf[ErrorInfo]) =>
      val v = any.unpack(classOf[ErrorInfo])
      ErrorInfoDetail(v.getReason, v.getMetadataMap.asScala.toMap)

    case any if any.is(classOf[RetryInfo]) =>
      val v = any.unpack(classOf[RetryInfo])
      val delay = v.getRetryDelay
      val duration = (delay.getSeconds.seconds + delay.getNanos.nanos).toCoarsest
      RetryInfoDetail(duration)

    case any if any.is(classOf[RequestInfo]) =>
      val v = any.unpack(classOf[RequestInfo])
      RequestInfoDetail(v.getRequestId)

    case any => throw new IllegalStateException(s"Could not unpack value of: |$any|")
  }

  def isInternalError(t: Throwable): Boolean = t match {
    case e: StatusRuntimeException => isInternalError(e)
    case _ => false
  }

  def isInternalError(e: StatusRuntimeException): Boolean =
    e.getStatus.getCode == Status.Code.INTERNAL && e.getStatus.getDescription.startsWith(
      BaseError.SecuritySensitiveMessageOnApiPrefix
    )

  /** @return whether a status runtime exception matches the error code.
    *
    * NOTE: This method is not suitable for:
    * 1) security sensitive error codes (e.g. internal or authentication related) as they are stripped from all the details when being converted to instances of [[StatusRuntimeException]],
    * 2) error codes that do not translate to gRPC level errors (i.e. error codes that don't have a corresponding gRPC status)
    */
  def matches(e: StatusRuntimeException, errorCode: ErrorCode): Boolean = {
    val matchesErrorCodeId = from(e).exists {
      case ErrorInfoDetail(errorCodeId, _) => errorCodeId == errorCode.id
      case _ => false
    }
    val matchesMessagePrefix = Option(e.getStatus.getDescription).exists(_.startsWith(errorCode.id))
    val matchesStatusCode = errorCode.category.grpcCode.contains(e.getStatus.getCode)
    matchesErrorCodeId && matchesMessagePrefix && matchesStatusCode
  }

  def matches(t: Throwable, errorCode: ErrorCode): Boolean = t match {
    case e: StatusRuntimeException => matches(e, errorCode)
    case _ => false
  }

}
