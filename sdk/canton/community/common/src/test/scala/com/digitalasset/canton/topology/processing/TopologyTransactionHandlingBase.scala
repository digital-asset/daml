// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

/** Base class for [[TopologyTransactionProcessorTest]] and
  * [[InitialTopologySnapshotValidatorTest]].
  */
abstract class TopologyTransactionHandlingBase
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasTestCloseContext {

  protected val crypto = new SymbolicPureCrypto()
  object Factory extends TopologyTransactionTestFactory(loggerFactory, parallelExecutionContext)

  protected def mkStore(
      synchronizerId: PhysicalSynchronizerId = Factory.physicalSynchronizerId1a,
      testName: String,
  ): TopologyStore[TopologyStoreId.SynchronizerStore]

  protected def ts(idx: Int): CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(idx.toLong)
  protected def fetch(
      store: TopologyStore[TopologyStoreId],
      timestamp: CantonTimestamp,
      isProposal: Boolean = false,
  ): List[TopologyMapping] =
    fetchTx(store, timestamp, isProposal).toTopologyState

  protected def fetchTx(
      store: TopologyStore[TopologyStoreId],
      timestamp: CantonTimestamp,
      isProposal: Boolean = false,
  ): GenericStoredTopologyTransactions =
    store
      .findPositiveTransactions(
        asOf = timestamp,
        asOfInclusive = false,
        isProposal = isProposal,
        types = TopologyMapping.Code.all,
        None,
        None,
      )
      .futureValueUS

  protected def validate(
      observed: Seq[TopologyMapping],
      expected: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ): Assertion = {
    if (observed.toSet != expected.map(_.mapping).toSet) {
      logger.debug("OBSERVED\n  " + observed.mkString("  \n"))
      logger.debug("EXPECTED\n  " + expected.map(_.mapping).mkString("  \n"))
    }
    observed.toSet shouldBe expected.map(_.mapping).toSet
  }

}
