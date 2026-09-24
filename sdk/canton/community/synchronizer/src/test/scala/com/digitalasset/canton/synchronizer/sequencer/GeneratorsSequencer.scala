// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.config.GeneratorsConfig.*
import com.digitalasset.canton.crypto.GeneratorsCrypto.*
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.GeneratorsDataTime.*
import com.digitalasset.canton.protocol.{GeneratorsProtocol, StaticSynchronizerParameters}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationId,
  AggregationRule,
  GeneratorsProtocol as GeneratorsProtocolSeq,
}
import com.digitalasset.canton.sequencing.traffic.{TrafficConsumed, TrafficPurchased}
import com.digitalasset.canton.synchronizer.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.synchronizer.sequencer.store.VersionedStatus
import com.digitalasset.canton.synchronizer.sequencing.integrations.state.DbSequencerStateManagerStore.AggregatedSignaturesOfSender
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.GeneratorsTransaction
import com.digitalasset.canton.topology.{GeneratorsTopology, Member}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.rpc.status.Status
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters.*

final class GeneratorsSequencer(
    protocolVersion: ProtocolVersion,
    generatorsTopology: GeneratorsTopology,
    generatorsTransaction: GeneratorsTransaction,
    generatorsProtocolSeq: GeneratorsProtocolSeq,
    generatorsProtocol: GeneratorsProtocol,
) {
  import generatorsTopology.*
  import generatorsTransaction.*
  import generatorsProtocolSeq.*
  import generatorsProtocol.*

  implicit val inFlightAggregationArb: Arbitrary[InFlightAggregation] = Arbitrary(
    for {
      rule <- Arbitrary.arbitrary[AggregationRule]
      maxSequencingTimestamp <- Arbitrary.arbitrary[CantonTimestamp]
      signatures <- boundedListGen(boundedListGen[Signature])
      aggregatedSendersList <- Gen.sequence(
        rule.eligibleSenders.forgetNE.map(member =>
          Gen
            .choose(CantonTimestamp.MinValue, maxSequencingTimestamp)
            .map(sequencingTimestamp =>
              member -> AggregationBySender(sequencingTimestamp, signatures)
            )
        )
      )
      aggregatedSenders = SortedMap.from(aggregatedSendersList.asScala)
    } yield InFlightAggregation.tryCreate(
      aggregatedSenders,
      maxSequencingTimestamp,
      rule,
    )
  )

  implicit val sequencerPruningStatusArb: Arbitrary[SequencerPruningStatus] = Arbitrary(
    for {
      lowerBound <- Arbitrary.arbitrary[CantonTimestamp]
      now <- Arbitrary.arbitrary[CantonTimestamp]
      members <- boundedListGen[SequencerMemberStatus]
    } yield SequencerPruningStatus(lowerBound, now, members.toSet)
  )

  implicit val sequencerSnapshotArb: Arbitrary[SequencerSnapshot] = Arbitrary(
    for {
      lastTs <- Arbitrary.arbitrary[CantonTimestamp]
      latestBlockHeight <- Arbitrary.arbitrary[Long]
      previousTimestamps <- boundedMapGen[Member, Option[CantonTimestamp]]
      status <- Arbitrary.arbitrary[SequencerPruningStatus]
      inFlightAggregations <- boundedMapGen[AggregationId, InFlightAggregation]
      additional <- Arbitrary.arbitrary[Option[SequencerSnapshot.ImplementationSpecificInfo]]
      trafficPurchased <- boundedListGen[TrafficPurchased]
      trafficConsumed <- boundedListGen[TrafficConsumed]
    } yield SequencerSnapshot(
      lastTs,
      latestBlockHeight,
      previousTimestamps,
      status,
      inFlightAggregations,
      additional,
      protocolVersion,
      trafficPurchased,
      trafficConsumed,
    )
  )

  implicit val onboardingStateForSequencer: Arbitrary[OnboardingStateForSequencer] = Arbitrary(
    for {
      // limit the transactions to two, otherwise the test runs become quite time-expensive.
      transactions <- boundedListGen[GenericStoredTopologyTransaction]
      topologySnapshot = StoredTopologyTransactions(transactions)
      staticSynchronizerParameters <- Arbitrary.arbitrary[StaticSynchronizerParameters]
      sequencerSnapshot <- Arbitrary.arbitrary[SequencerSnapshot]
    } yield OnboardingStateForSequencer(
      topologySnapshot,
      staticSynchronizerParameters,
      sequencerSnapshot,
    )
  )

  implicit val onboardingStateForSequencerV2: Arbitrary[OnboardingStateForSequencerV2] = Arbitrary(
    for {
      topologySnapshotO <- Gen.option(Arbitrary.arbitrary[GenericStoredTopologyTransaction])
      staticSynchronizerParametersO <- Gen.option(Arbitrary.arbitrary[StaticSynchronizerParameters])
      sequencerSnapshotO <- Gen.option(Arbitrary.arbitrary[SequencerSnapshot])
    } yield OnboardingStateForSequencerV2(
      topologySnapshotO,
      staticSynchronizerParametersO,
      sequencerSnapshotO,
      protocolVersion,
    )
  )

  implicit val versionedStatusArb: Arbitrary[VersionedStatus] = {
    implicit val protoAnyArb: Arbitrary[com.google.protobuf.any.Any] = Arbitrary(
      for {
        typeUrl <- Arbitrary.arbString.arbitrary
        value <- Arbitrary.arbString.arbitrary.map(com.google.protobuf.ByteString.copyFromUtf8)
      } yield com.google.protobuf.any.Any(typeUrl, value)
    )

    Arbitrary(
      for {
        code <- Gen.oneOf(io.grpc.Status.Code.values().toIndexedSeq).map(_.value())
        message <- Arbitrary.arbString.arbitrary
        details <- boundedListGen[com.google.protobuf.any.Any]
      } yield {
        val status = Status(
          code = code,
          message = message,
          details = details,
        )
        VersionedStatus.create(status, protocolVersion)
      }
    )
  }

  implicit val aggregatedSignaturesOfSenderArb: Arbitrary[AggregatedSignaturesOfSender] = Arbitrary(
    boundedListGen[List[Signature]](boundedListGen[Signature]).map(
      AggregatedSignaturesOfSender(_)(
        AggregatedSignaturesOfSender.protocolVersionRepresentativeFor(protocolVersion)
      )
    )
  )
}
