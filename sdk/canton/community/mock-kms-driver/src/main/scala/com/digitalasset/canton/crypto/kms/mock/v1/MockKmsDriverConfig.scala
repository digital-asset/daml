// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.v1

import com.digitalasset.canton.config
import com.digitalasset.canton.crypto.KeyName

/** Configuration for the mock KMS driver.
  *
  * @param auditLogging
  *   Enable request and response logging to simulate audit logging in a real KMS.
  * @param signingLatencies
  *   For testing we allow to simulate the latency for signing operations based on the key name.
  */
final case class MockKmsDriverConfig(
    auditLogging: Boolean = false,
    signingLatencies: Map[KeyName, config.NonNegativeFiniteDuration] = Map.empty,
)
