// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.crypto.BaseCrypto
import com.digitalasset.canton.topology.store.TopologyStoreId

trait TopologyManagerStatus {
  def queueSize: Int
}

object TopologyManagerStatus {

  /** @param managers
    *   a collection of topology managers. It uses [[com.digitalasset.canton.crypto.BaseCrypto]]
    *   because it may include different types of
    *   [[com.digitalasset.canton.topology.TopologyManager]]s, such as
    *   [[com.digitalasset.canton.topology.LocalTopologyManager]] or
    *   [[com.digitalasset.canton.topology.SynchronizerTopologyManager]], which rely on different
    *   crypto types.
    */
  def combined(
      managers: TopologyManager[TopologyStoreId, BaseCrypto]*
  ): TopologyManagerStatus =
    new TopologyManagerStatus {
      override def queueSize: Int = managers.map(_.queueSize).sum
    }
}
