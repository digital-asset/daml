// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.ErrorCode
import com.google.protobuf
import com.google.rpc.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

object ErrorDetails {

  sealed trait ErrorDetail extends Product with Serializable {
    type T <: com.google.protobuf.Message
    def toRpc: T
    def toRpcAny: com.google.protobuf.Any = com.google.protobuf.Any.pack(toRpc)
  }

  final case class ResourceInfoDetail(name: String, typ: String) extends ErrorDetail {
    type T = ResourceInfo
    def toRpc: ResourceInfo = {
      ResourceInfo.newBuilder().setResourceType(typ).setResourceName(name).build()
    }
  }
  final case class ErrorInfoDetail(errorCodeId: String, metadata: Map[String, String])
      extends ErrorDetail {
    type T = ErrorInfo
    def toRpc: ErrorInfo = {
      ErrorInfo
        .newBuilder()
        .setReason(errorCodeId)
        .putAllMetadata(metadata.asJava)
        .build()
    }
  }
  final case class RetryInfoDetail(duration: Duration) extends ErrorDetail {
    type T = RetryInfo
    def toRpc: RetryInfo = {
      val millis = duration.toMillis
      val fullSeconds = millis / 1000
      val remainderMillis = millis % 1000
      // Ensuring that we do not exceed max allowed value of nanos as documented in [[com.google.protobuf.Duration.Builder.setNanos]]
      val remainderNanos = Math.min(remainderMillis * 1000 * 1000, 999999999).toInt
      val protoDuration = com.google.protobuf.Duration
        .newBuilder()
        .setNanos(remainderNanos)
        .setSeconds(fullSeconds)
        .build()
      RetryInfo
        .newBuilder()
        .setRetryDelay(protoDuration)
        .build()
    }
  }
  final case class RequestInfoDetail(correlationId: String) extends ErrorDetail {
    type T = RequestInfo
    def toRpc: RequestInfo = {
      RequestInfo
        .newBuilder()
        .setRequestId(correlationId)
        .setServingData("")
        .build()
    }
  }

  def from(status: com.google.rpc.Status): Seq[ErrorDetail] = {
    from(status.getDetailsList.asScala.toSeq)
  }
  def from(e: StatusRuntimeException): Seq[ErrorDetail] =
    from(StatusProto.fromThrowable(e))

  def from(anys: Seq[protobuf.Any]): Seq[ErrorDetail] = anys.toList.map {
    case any if any.is(classOf[ResourceInfo]) =>
      val v = any.unpack(classOf[ResourceInfo])
      ResourceInfoDetail(typ = v.getResourceType, name = v.getResourceName)

    case any if any.is(classOf[ErrorInfo]) =>
      val v = any.unpack(classOf[ErrorInfo])
      ErrorInfoDetail(errorCodeId = v.getReason, metadata = v.getMetadataMap.asScala.toMap)

    case any if any.is(classOf[RetryInfo]) =>
      val v = any.unpack(classOf[RetryInfo])
      val delay = v.getRetryDelay
      val duration = (delay.getSeconds.seconds + delay.getNanos.nanos).toCoarsest
      RetryInfoDetail(duration = duration)

    case any if any.is(classOf[RequestInfo]) =>
      val v = any.unpack(classOf[RequestInfo])
      RequestInfoDetail(correlationId = v.getRequestId)

    case any => throw new IllegalStateException(s"Could not unpack value of: |$any|")
  }

  /** @return whether a status runtime exception matches the error code.
    *
    * NOTE: This method is not suitable for:
    * 1) security sensitive error codes (e.g. internal or authentication related) as they are stripped from all the details when being converted to instances of [[io.grpc.StatusRuntimeException]],
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
