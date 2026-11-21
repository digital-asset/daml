// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyStore.TopologyStoreDeactivations
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{TopologyChangeOp, TopologyMapping}
import com.digitalasset.canton.topology.{Namespace, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.{Assertion, Suite}

private[store] trait TopologyStoreTestBase extends BaseTest with HasExecutionContext {
  this: Suite & NamedLogging =>
  protected def update(
      store: TopologyStore[TopologyStoreId],
      ts: CantonTimestamp,
      add: Seq[GenericSignedTopologyTransaction] = Seq.empty,
      removals: TopologyStoreDeactivations = Map.empty,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    store.update(
      SequencedTime(ts),
      EffectiveTime(ts),
      removals = removals,
      add.map(ValidatedTopologyTransaction(_)),
    )

  protected def inspect(
      store: TopologyStore[TopologyStoreId],
      timeQuery: TimeQuery,
      proposals: Boolean = false,
      recentTimestampO: Option[CantonTimestamp] = None,
      op: Option[TopologyChangeOp] = None,
      types: Seq[TopologyMapping.Code] = Nil,
      idFilter: Option[String] = None,
      namespaceFilter: Option[String] = None,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] =
    store.inspect(
      proposals,
      timeQuery,
      recentTimestampO,
      op,
      types,
      idFilter,
      namespaceFilter,
    )

  protected def inspectKnownParties(
      store: TopologyStore[TopologyStoreId],
      timestamp: CantonTimestamp,
      filterParty: String = "",
      filterParticipant: String = "",
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    store.inspectKnownParties(
      timestamp,
      filterParty,
      filterParticipant,
      limit = Int.MaxValue, // we don't care about the limit in tests
    )

  protected def findPositiveTransactions(
      store: TopologyStore[TopologyStoreId],
      asOf: CantonTimestamp,
      asOfInclusive: Boolean = false,
      isProposal: Boolean = false,
      types: Seq[TopologyMapping.Code] = TopologyMapping.Code.all,
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]] = None,
      filterNamespace: Option[NonEmpty[Seq[Namespace]]] = None,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PositiveStoredTopologyTransactions] =
    store.findPositiveTransactions(
      asOf,
      asOfInclusive,
      isProposal,
      types,
      filterUid,
      filterNamespace,
    )

  protected def expectTransactions(
      actual: GenericStoredTopologyTransactions,
      expected: Seq[GenericSignedTopologyTransaction],
  ): Assertion = {
    logger.info(s"Actual: ${actual.result.map(_.transaction).mkString(",")}")
    logger.info(s"Expected: ${expected.mkString(",")}")
    // run more readable assert first since mapping codes are easier to identify than hashes ;-)
    actual.result.map(_.mapping.code.code) shouldBe expected.map(
      _.mapping.code.code
    )
    actual.result.map(_.hash) shouldBe expected.map(_.hash)
  }
}
