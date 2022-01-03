// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc

import com.google.rpc

object RpcProtoExtractors {

  object Exception {
    def unapply(exception: Exception): Option[rpc.Status] = {
      Option(io.grpc.protobuf.StatusProto.fromThrowable(exception))
    }
  }

  object Status {
    def unapply(status: com.google.rpc.Status): Option[com.google.rpc.Code] = {
      Some(com.google.rpc.Code.forNumber(status.getCode))
    }
  }
}
