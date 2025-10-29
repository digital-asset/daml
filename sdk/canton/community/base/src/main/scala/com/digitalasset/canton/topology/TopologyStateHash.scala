// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction

/** A utility class to compute a hash over the state of the topology store. Order of transactions
  * matters. Not thread-safe!
  */
class TopologyStateHash {
  private val builder = Hash.build(HashPurpose.InitialTopologyStateConsistency, Sha256)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var txCounter: Long = 0L

  def add(tx: GenericStoredTopologyTransaction): Unit = {
    txCounter += 1
    builder.addWithoutLengthPrefix(tx.hash.hash.getCryptographicEvidence)
  }

  def finish(): Hash =
    builder.add(txCounter).finish()
}
