// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import io.grpc.reflection.v1.ServerReflectionRequest

private[reflection] object ServerReflectionRequests {

  val ListServices: ServerReflectionRequest =
    ServerReflectionRequest.newBuilder().setListServices("").build()

  def fileContaining(symbol: String): ServerReflectionRequest =
    ServerReflectionRequest.newBuilder().setFileContainingSymbol(symbol).build()

}
