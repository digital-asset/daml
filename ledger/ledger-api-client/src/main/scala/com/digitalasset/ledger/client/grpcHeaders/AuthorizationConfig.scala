// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.grpcHeaders

sealed abstract class AuthorizationConfig extends Product with Serializable

object AuthorizationConfig {
  final case class FileAccessToken(filePath: String) extends AuthorizationConfig
  final case class LiveAccessToken(uri: String) extends AuthorizationConfig
}
