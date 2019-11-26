// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.client

import java.util.concurrent.Executor

import io.grpc.stub.AbstractStub
import io.grpc.{CallCredentials, Metadata}

object LedgerCallCredentials {

  private val header = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  def authenticatingStub[A <: AbstractStub[A]](stub: A)(token: String): A =
    stub.withCallCredentials(new LedgerCallCredentials(token))

}

final class LedgerCallCredentials(token: String) extends CallCredentials {

  override def applyRequestMetadata(
      requestInfo: CallCredentials.RequestInfo,
      appExecutor: Executor,
      applier: CallCredentials.MetadataApplier): Unit = {
    val metadata = new Metadata
    metadata.put(LedgerCallCredentials.header, token)
    applier.apply(metadata)
  }

  override def thisUsesUnstableApi(): Unit = ()

}
