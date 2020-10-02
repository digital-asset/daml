// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import pureconfig.error.ConfigReaderFailures

/**
  * This object collects the possible errors that can happen during command
  * submission or event listening
  */
object LedgerClientConfigurationError {
  sealed abstract class PlatformApiError(message: String) extends RuntimeException(message)

  final case class MalformedTypesafeConfig(failures: ConfigReaderFailures)
      extends PlatformApiError(s"Malformed configuration: $failures")
}
