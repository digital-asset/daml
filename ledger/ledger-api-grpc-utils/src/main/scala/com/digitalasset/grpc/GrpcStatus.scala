// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc

import io.grpc.Status
import com.google.rpc.status.{Status => ProtobufStatus}

object GrpcStatus {

  def unapply(arg: Status): Some[(Status.Code, Option[String])] =
    Some((arg.getCode, Option(arg.getDescription)))

  def toProto(code: Status.Code, description: Option[String]): ProtobufStatus =
    ProtobufStatus(code.value, description.getOrElse(""))

  def toProto(status: Status): ProtobufStatus =
    toProto(status.getCode, Option(status.getDescription))

}
