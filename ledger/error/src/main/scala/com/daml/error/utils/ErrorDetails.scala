// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.ErrorCode
import com.google.protobuf
import com.google.rpc.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import scala.jdk.CollectionConverters._

object ErrorDetails {
  sealed trait ErrorDetail extends Product with Serializable

  final case class ResourceInfoDetail(name: String, typ: String) extends ErrorDetail
  final case class ErrorInfoDetail(reason: String, metadata: Map[String, String] = Map.empty)
      extends ErrorDetail
  final case class RetryInfoDetail(retryDelayInSeconds: Long) extends ErrorDetail
  final case class RequestInfoDetail(requestId: String) extends ErrorDetail

  def from(anys: Seq[protobuf.Any]): Seq[ErrorDetail] = anys.toList.flatMap {
    case any if any.is(classOf[ResourceInfo]) =>
      val v = any.unpack(classOf[ResourceInfo])
      List(ResourceInfoDetail(v.getResourceType, v.getResourceName))

    case any if any.is(classOf[ErrorInfo]) =>
      val v = any.unpack(classOf[ErrorInfo])
      val reason = v.getReason
      if (reason.nonEmpty || v.getMetadataCount > 0)
        List(ErrorInfoDetail(reason, v.getMetadataMap.asScala.toMap))
      else Nil

    case any if any.is(classOf[RetryInfo]) =>
      val v = any.unpack(classOf[RetryInfo])
      List(RetryInfoDetail(v.getRetryDelay.getSeconds))

    case any if any.is(classOf[RequestInfo]) =>
      val v = any.unpack(classOf[RequestInfo])
      List(RequestInfoDetail(v.getRequestId))

    case any => throw new IllegalStateException(s"Could not unpack value of: |$any|")
  }

  def isErrorCode(exception: StatusRuntimeException)(errorCode: ErrorCode): Boolean = {
    val rpcStatus =
      StatusProto.fromStatusAndTrailers(exception.getStatus, exception.getTrailers)

    ErrorDetails
      .from(rpcStatus.getDetailsList.asScala.toSeq)
      .exists {
        case ErrorInfoDetail(reason, _) => reason == errorCode.id
        case _ => false
      }
  }
}
