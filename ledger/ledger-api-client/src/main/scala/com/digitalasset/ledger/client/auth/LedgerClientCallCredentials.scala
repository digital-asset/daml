// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.auth

import java.util.concurrent.Executor

import io.grpc.stub.AbstractStub
import io.grpc.{CallCredentials, Metadata}

object LedgerClientCallCredentials {

  private val header = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  def authenticatingStub[A <: AbstractStub[A]](stub: A)(token: String): A =
    stub.withCallCredentials(new LedgerClientCallCredentials(token))

}

final class LedgerClientCallCredentials(token: String) extends CallCredentials {

  override def applyRequestMetadata(
      requestInfo: CallCredentials.RequestInfo,
      appExecutor: Executor,
      applier: CallCredentials.MetadataApplier): Unit = {
    val metadata = new Metadata
    metadata.put(LedgerClientCallCredentials.header, token)
    applier.apply(metadata)
  }

  override def thisUsesUnstableApi(): Unit = ()

}
