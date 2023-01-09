// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.reflection

import io.grpc.reflection.v1alpha.ServerReflectionRequest

private[reflection] object ServerReflectionRequests {

  val ListServices: ServerReflectionRequest =
    ServerReflectionRequest.newBuilder().setListServices("").build()

  def fileContaining(symbol: String): ServerReflectionRequest =
    ServerReflectionRequest.newBuilder().setFileContainingSymbol(symbol).build()

}
