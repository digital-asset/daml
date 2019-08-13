// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

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
