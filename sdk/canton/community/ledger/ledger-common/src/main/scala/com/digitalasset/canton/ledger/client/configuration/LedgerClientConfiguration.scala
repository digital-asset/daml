// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.configuration

/** @param userId
  *   The string that will be used as an user identifier when issuing commands and retrieving
  *   transactions
  * @param commandClient
  *   The [[CommandClientConfiguration]] that defines how the command client should be setup with
  *   regards to timeouts, commands in-flight and command TTL
  * @param token
  *   If defined, the access token that will be passed by default, unless overridden in individual
  *   calls (mostly useful for short-lived applications)
  */
final case class LedgerClientConfiguration(
    userId: String,
    commandClient: CommandClientConfiguration,
    token: Option[String] = None,
)
