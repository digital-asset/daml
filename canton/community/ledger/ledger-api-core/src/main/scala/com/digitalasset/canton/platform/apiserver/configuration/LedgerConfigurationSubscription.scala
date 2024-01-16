// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import com.digitalasset.canton.ledger.configuration.Configuration

/** Makes the current ledger configuration available in a centralized place. */
trait LedgerConfigurationSubscription {

  /** The latest configuration found so far. There may be a delay between an update to the ledger
    * configuration and that configuration becoming available through this method.
    */
  def latestConfiguration(): Option[Configuration]
}
