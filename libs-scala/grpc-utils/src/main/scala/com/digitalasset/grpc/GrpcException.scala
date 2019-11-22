// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc

import com.digitalasset.grpc.GrpcStatus.SpecificGrpcStatus
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

  val OK = SpecificGrpcException(GrpcStatus.OK)
  val CANCELLED = SpecificGrpcException(GrpcStatus.CANCELLED)
  val UNKNOWN = SpecificGrpcException(GrpcStatus.UNKNOWN)
  val INVALID_ARGUMENT = SpecificGrpcException(GrpcStatus.INVALID_ARGUMENT)
  val DEADLINE_EXCEEDED = SpecificGrpcException(GrpcStatus.DEADLINE_EXCEEDED)
  val NOT_FOUND = SpecificGrpcException(GrpcStatus.NOT_FOUND)
  val ALREADY_EXISTS = SpecificGrpcException(GrpcStatus.ALREADY_EXISTS)
  val PERMISSION_DENIED = SpecificGrpcException(GrpcStatus.PERMISSION_DENIED)
  val RESOURCE_EXHAUSTED = SpecificGrpcException(GrpcStatus.RESOURCE_EXHAUSTED)
  val FAILED_PRECONDITION = SpecificGrpcException(GrpcStatus.FAILED_PRECONDITION)
  val ABORTED = SpecificGrpcException(GrpcStatus.ABORTED)
  val OUT_OF_RANGE = SpecificGrpcException(GrpcStatus.OUT_OF_RANGE)
  val UNIMPLEMENTED = SpecificGrpcException(GrpcStatus.UNIMPLEMENTED)
  val INTERNAL = SpecificGrpcException(GrpcStatus.INTERNAL)
  val UNAVAILABLE = SpecificGrpcException(GrpcStatus.UNAVAILABLE)
  val DATA_LOSS = SpecificGrpcException(GrpcStatus.DATA_LOSS)
  val UNAUTHENTICATED = SpecificGrpcException(GrpcStatus.UNAUTHENTICATED)
}
