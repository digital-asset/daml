// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc

import com.google.rpc.status.{Status => ProtobufStatus}
import io.grpc.Status
import io.grpc.Status.Code

object GrpcStatus {
  type Description = Option[String]

  def unapply(status: Status): Some[(Code, Description)] =
    Some((status.getCode, Option(status.getDescription)))

  def toProto(code: Code, description: Description): ProtobufStatus =
    ProtobufStatus(code.value, description.getOrElse(""))

  def toProto(status: Status): ProtobufStatus =
    toProto(status.getCode, Option(status.getDescription))

  case class SpecificGrpcStatus(code: Code) {
    def unapply(status: Status): Boolean =
      status.getCode == code
  }

  val OK = SpecificGrpcStatus(Code.OK)
  val CANCELLED = SpecificGrpcStatus(Code.CANCELLED)
  val UNKNOWN = SpecificGrpcStatus(Code.UNKNOWN)
  val INVALID_ARGUMENT = SpecificGrpcStatus(Code.INVALID_ARGUMENT)
  val DEADLINE_EXCEEDED = SpecificGrpcStatus(Code.DEADLINE_EXCEEDED)
  val NOT_FOUND = SpecificGrpcStatus(Code.NOT_FOUND)
  val ALREADY_EXISTS = SpecificGrpcStatus(Code.ALREADY_EXISTS)
  val PERMISSION_DENIED = SpecificGrpcStatus(Code.PERMISSION_DENIED)
  val RESOURCE_EXHAUSTED = SpecificGrpcStatus(Code.RESOURCE_EXHAUSTED)
  val FAILED_PRECONDITION = SpecificGrpcStatus(Code.FAILED_PRECONDITION)
  val ABORTED = SpecificGrpcStatus(Code.ABORTED)
  val OUT_OF_RANGE = SpecificGrpcStatus(Code.OUT_OF_RANGE)
  val UNIMPLEMENTED = SpecificGrpcStatus(Code.UNIMPLEMENTED)
  val INTERNAL = SpecificGrpcStatus(Code.INTERNAL)
  val UNAVAILABLE = SpecificGrpcStatus(Code.UNAVAILABLE)
  val DATA_LOSS = SpecificGrpcStatus(Code.DATA_LOSS)
  val UNAUTHENTICATED = SpecificGrpcStatus(Code.UNAUTHENTICATED)
}
