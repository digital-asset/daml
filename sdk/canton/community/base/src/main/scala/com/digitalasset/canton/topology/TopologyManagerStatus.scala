// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.topology.store.TopologyStoreId

trait TopologyManagerStatus {
  def queueSize: Int
}

object TopologyManagerStatus {
  def combined[PureCrypto <: CryptoPureApi](
      managers: TopologyManager[TopologyStoreId, PureCrypto]*
  ): TopologyManagerStatus =
    new TopologyManagerStatus {
      override def queueSize: Int = managers.map(_.queueSize).sum
    }
}
