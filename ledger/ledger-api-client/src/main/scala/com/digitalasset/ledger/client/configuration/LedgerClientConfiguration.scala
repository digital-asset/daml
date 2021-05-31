// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.configuration

import io.grpc.internal.GrpcUtil
import io.netty.handler.ssl.SslContext

/** @param applicationId          The string that will be used as an application identifier when issuing commands and retrieving transactions
  * @param ledgerIdRequirement    A [[LedgerIdRequirement]] specifying how the ledger identifier must be checked against the one returned by the LedgerIdentityService
  * @param commandClient          The [[CommandClientConfiguration]] that defines how the command client should be setup with regards to timeouts, commands in flight and command TTL
  * @param sslContext             If defined, the context will be passed on to the underlying gRPC code to ensure the communication channel is secured by TLS
  * @param token                  If defined, the access token that will be passed by default, unless overridden in individual calls (mostly useful for short-lived applications)
  * @param maxInboundMetadataSize The maximum size of the response headers.
  * @param maxInboundMessageSize  The maximum (uncompressed) size of the response body.
  */
final case class LedgerClientConfiguration(
    applicationId: String,
    ledgerIdRequirement: LedgerIdRequirement,
    commandClient: CommandClientConfiguration,
    sslContext: Option[SslContext],
    token: Option[String] = None,
    maxInboundMetadataSize: Int = GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE,
    maxInboundMessageSize: Int = GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE,
)
