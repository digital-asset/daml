// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.configuration

import java.time.Duration

/** Ledger configuration describing the ledger's time model.
  *
  * @param maxDeduplicationDuration  The maximum time window during which commands can be deduplicated.
  */
final case class Configuration(
    maxDeduplicationDuration: Duration
)
