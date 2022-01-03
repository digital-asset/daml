// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import com.daml.grpc.GrpcStatus.SpecificGrpcStatus
import io.grpc.{Metadata, Status, StatusException, StatusRuntimeException}

object GrpcException {
  def unapply(exception: Exception): Option[(Status, Metadata)] =
    exception match {
      case e: StatusRuntimeException => Some((e.getStatus, e.getTrailers))
      case e: StatusException => Some((e.getStatus, e.getTrailers))
      case _ => None
    }

  private[grpc] final class SpecificGrpcException(status: SpecificGrpcStatus) {
    def unapply(exception: Exception): Boolean =
      exception match {
        case e: StatusRuntimeException => status.unapply(e.getStatus)
        case e: StatusException => status.unapply(e.getStatus)
        case _ => false
      }
  }

  val OK = new SpecificGrpcException(GrpcStatus.OK)
  val CANCELLED = new SpecificGrpcException(GrpcStatus.CANCELLED)
  val UNKNOWN = new SpecificGrpcException(GrpcStatus.UNKNOWN)
  val INVALID_ARGUMENT = new SpecificGrpcException(GrpcStatus.INVALID_ARGUMENT)
  val DEADLINE_EXCEEDED = new SpecificGrpcException(GrpcStatus.DEADLINE_EXCEEDED)
  val NOT_FOUND = new SpecificGrpcException(GrpcStatus.NOT_FOUND)
  val ALREADY_EXISTS = new SpecificGrpcException(GrpcStatus.ALREADY_EXISTS)
  val PERMISSION_DENIED = new SpecificGrpcException(GrpcStatus.PERMISSION_DENIED)
  val RESOURCE_EXHAUSTED = new SpecificGrpcException(GrpcStatus.RESOURCE_EXHAUSTED)
  val FAILED_PRECONDITION = new SpecificGrpcException(GrpcStatus.FAILED_PRECONDITION)
  val ABORTED = new SpecificGrpcException(GrpcStatus.ABORTED)
  val OUT_OF_RANGE = new SpecificGrpcException(GrpcStatus.OUT_OF_RANGE)
  val UNIMPLEMENTED = new SpecificGrpcException(GrpcStatus.UNIMPLEMENTED)
  val INTERNAL = new SpecificGrpcException(GrpcStatus.INTERNAL)
  val UNAVAILABLE = new SpecificGrpcException(GrpcStatus.UNAVAILABLE)
  val DATA_LOSS = new SpecificGrpcException(GrpcStatus.DATA_LOSS)
  val UNAUTHENTICATED = new SpecificGrpcException(GrpcStatus.UNAUTHENTICATED)
}
