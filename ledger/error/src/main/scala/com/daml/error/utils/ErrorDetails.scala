// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.google.protobuf
import com.google.rpc.{ErrorInfo, RequestInfo, ResourceInfo, RetryInfo}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

object ErrorDetails {
  sealed trait ErrorDetail extends Product with Serializable

  final case class ResourceInfoDetail(name: String, typ: String) extends ErrorDetail
  final case class ErrorInfoDetail(reason: String, metadata: Map[String, String])
      extends ErrorDetail
  final case class RetryInfoDetail(duration: Duration) extends ErrorDetail
  final case class RequestInfoDetail(requestId: String) extends ErrorDetail

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
}
