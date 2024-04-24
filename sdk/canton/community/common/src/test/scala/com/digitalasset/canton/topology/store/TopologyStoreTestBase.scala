// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.transaction.{TopologyChangeOp, TopologyMapping}
import com.digitalasset.canton.topology.{Namespace, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.{Assertion, Suite}

import scala.concurrent.Future

private[store] trait TopologyStoreTestBase extends BaseTest with HasExecutionContext {
  this: Suite & NamedLogging =>
  protected def update(
      store: TopologyStore[TopologyStoreId],
      ts: CantonTimestamp,
      add: Seq[GenericSignedTopologyTransaction] = Seq.empty,
      removeMapping: Map[MappingHash, PositiveInt] = Map.empty,
      removeTxs: Set[TxHash] = Set.empty,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store.update(
      SequencedTime(ts),
      EffectiveTime(ts),
      removeMapping,
      removeTxs,
      add.map(ValidatedTopologyTransaction(_)),
    )
  }

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
  ): Future[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] =
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
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] =
    store.inspectKnownParties(
      timestamp,
      filterParty,
      filterParticipant,
      limit = 1000,
    )

  protected def findPositiveTransactions(
      store: TopologyStore[TopologyStoreId],
      asOf: CantonTimestamp,
      asOfInclusive: Boolean = false,
      isProposal: Boolean = false,
      types: Seq[TopologyMapping.Code] = TopologyMapping.Code.all,
      filterUid: Option[Seq[UniqueIdentifier]] = None,
      filterNamespace: Option[Seq[Namespace]] = None,
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions] =
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
