// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

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

  private[grpc] final class SpecificGrpcStatus(code: Code) {
    def unapply(status: Status): Boolean =
      status.getCode == code
  }

  val OK = new SpecificGrpcStatus(Code.OK)
  val CANCELLED = new SpecificGrpcStatus(Code.CANCELLED)
  val UNKNOWN = new SpecificGrpcStatus(Code.UNKNOWN)
  val INVALID_ARGUMENT = new SpecificGrpcStatus(Code.INVALID_ARGUMENT)
  val DEADLINE_EXCEEDED = new SpecificGrpcStatus(Code.DEADLINE_EXCEEDED)
  val NOT_FOUND = new SpecificGrpcStatus(Code.NOT_FOUND)
  val ALREADY_EXISTS = new SpecificGrpcStatus(Code.ALREADY_EXISTS)
  val PERMISSION_DENIED = new SpecificGrpcStatus(Code.PERMISSION_DENIED)
  val RESOURCE_EXHAUSTED = new SpecificGrpcStatus(Code.RESOURCE_EXHAUSTED)
  val FAILED_PRECONDITION = new SpecificGrpcStatus(Code.FAILED_PRECONDITION)
  val ABORTED = new SpecificGrpcStatus(Code.ABORTED)
  val OUT_OF_RANGE = new SpecificGrpcStatus(Code.OUT_OF_RANGE)
  val UNIMPLEMENTED = new SpecificGrpcStatus(Code.UNIMPLEMENTED)
  val INTERNAL = new SpecificGrpcStatus(Code.INTERNAL)
  val UNAVAILABLE = new SpecificGrpcStatus(Code.UNAVAILABLE)
  val DATA_LOSS = new SpecificGrpcStatus(Code.DATA_LOSS)
  val UNAUTHENTICATED = new SpecificGrpcStatus(Code.UNAUTHENTICATED)
}
