// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.protocol.v30
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.*
import slick.jdbc.SetParameter

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

/** Database store for server side sequencer data.
  * If you need more than one sequencer running on the same db, you can isolate them using
  * different sequencerStoreIds. This is useful for tests and for sequencer applications that implement multiple domains.
  */
class DbSequencerStateManagerStore(
    override protected val storage: DbStorage,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerStateManagerStore
    with DbStore {

  import DbSequencerStateManagerStore.*
  import Member.DbStorageImplicits.*
  import storage.api.*
  import storage.converters.*

  override def readInFlightAggregations(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[InFlightAggregations] =
    storage.query(readInFlightAggregationsDBIO(timestamp), functionFullName)

  /** Compute the state up until (inclusive) the given timestamp. */
  def readInFlightAggregationsDBIO(
      timestamp: CantonTimestamp
  ): DBIOAction[
    InFlightAggregations,
    NoStream,
    Effect.Read with Effect.Transactional,
  ] = {
    val aggregationsQ =
      sql"""
            select seq_in_flight_aggregation.aggregation_id,
                   seq_in_flight_aggregation.max_sequencing_time,
                   seq_in_flight_aggregation.aggregation_rule,
                   seq_in_flight_aggregated_sender.sender,
                   seq_in_flight_aggregated_sender.sequencing_timestamp,
                   seq_in_flight_aggregated_sender.signatures
            from seq_in_flight_aggregation inner join seq_in_flight_aggregated_sender on seq_in_flight_aggregation.aggregation_id = seq_in_flight_aggregated_sender.aggregation_id
            where seq_in_flight_aggregation.max_sequencing_time > $timestamp and seq_in_flight_aggregated_sender.sequencing_timestamp <= $timestamp
          """.as[
        (
            AggregationId,
            CantonTimestamp,
            AggregationRule,
            Member,
            CantonTimestamp,
            AggregatedSignaturesOfSender,
        )
      ]
    aggregationsQ.map { aggregations =>
      val byAggregationId = aggregations.groupBy { case (aggregationId, _, _, _, _, _) =>
        aggregationId
      }
      byAggregationId.fmap { aggregationsForId =>
        val aggregationsNE = NonEmptyUtil.fromUnsafe(aggregationsForId)
        val (_, maxSequencingTimestamp, aggregationRule, _, _, _) =
          aggregationsNE.head1
        val aggregatedSenders = aggregationsNE.map {
          case (_, _, _, sender, sequencingTimestamp, signatures) =>
            sender -> AggregationBySender(sequencingTimestamp, signatures.signaturesByEnvelope)
        }.toMap
        InFlightAggregation
          .create(aggregatedSenders, maxSequencingTimestamp, aggregationRule)
          .valueOr(err => throw new DbDeserializationException(err))
      }
    }
  }

  override def addInFlightAggregationUpdates(updates: InFlightAggregationUpdates)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.queryAndUpdate(
      addInFlightAggregationUpdatesDBIO(updates),
      functionFullName,
    )

  private[domain] def addInFlightAggregationUpdatesDBIO(updates: InFlightAggregationUpdates)(
      implicit traceContext: TraceContext
  ): DBIO[Unit] = {
    // First add all aggregation ids with their expiry timestamp and rules,
    // then add the information about the aggregated senders.

    val addAggregationIdsQ = storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into seq_in_flight_aggregation(aggregation_id, max_sequencing_time, aggregation_rule)
           values (?, ?, ?)
           on conflict do nothing
           """
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( seq_in_flight_aggregation ( aggregation_id ) ) */
          into seq_in_flight_aggregation ifa
          using (select ? aggregation_id, ? max_sequencing_time, ? aggregation_rule from dual) input
          on (ifa.aggregation_id = input.aggregation_id)
          when not matched then
            insert (aggregation_id, max_sequencing_time, aggregation_rule)
            values (input.aggregation_id, input.max_sequencing_time, input.aggregation_rule)
          """
    }
    implicit val setParameterAggregationRule: SetParameter[AggregationRule] =
      AggregationRule.getVersionedSetParameter
    val freshAggregations = updates
      .to(immutable.Iterable)
      .flatMap { case (aggregationId, updateForId) =>
        updateForId.freshAggregation.map(aggregationId -> _).toList
      }
    val addAggregationIdsDbio =
      DbStorage.bulkOperation_(addAggregationIdsQ, freshAggregations, storage.profile) {
        pp => agg =>
          val (aggregationId, FreshInFlightAggregation(maxSequencingTimestamp, rule)) = agg
          pp.>>(aggregationId)
          pp.>>(maxSequencingTimestamp)
          pp.>>(rule)
      }

    val addSendersQ = storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into seq_in_flight_aggregated_sender(aggregation_id, sender, sequencing_timestamp, signatures)
           values (?, ?, ?, ?)
           on conflict do nothing"""
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( seq_in_flight_aggregated_sender ( aggregation_id, sender ) ) */
           into seq_in_flight_aggregated_sender ifas
           using (select ? aggregation_id, ? sender, ? sequencing_timestamp, ? signatures from dual) input
           on (ifas.aggregation_id = input.aggregation_id and ifas.sender = input.sender)
           when not matched then
             insert (aggregation_id, sender, sequencing_timestamp, signatures)
             values (input.aggregation_id, input.sender, input.sequencing_timestamp, input.signatures)
       """
    }
    implicit val setParameterAggregatedSignaturesOfSender
        : SetParameter[AggregatedSignaturesOfSender] =
      AggregatedSignaturesOfSender.getVersionedSetParameter
    val aggregatedSenders =
      updates.to(immutable.Iterable).flatMap { case (aggregationId, updateForId) =>
        updateForId.aggregatedSenders.map(aggregationId -> _).iterator
      }
    val addSendersDbIO = DbStorage.bulkOperation_(addSendersQ, aggregatedSenders, storage.profile) {
      pp => item =>
        val (aggregationId, AggregatedSender(sender, aggregation)) = item
        pp.>>(aggregationId)
        pp.>>(sender)
        pp.>>(aggregation.sequencingTimestamp)
        pp.>>(
          AggregatedSignaturesOfSender(aggregation.signatures)(
            AggregatedSignaturesOfSender.protocolVersionRepresentativeFor(protocolVersion)
          )
        )
    }

    // Flatmap instead of zip because we first must insert the aggregations and only later the senders due to the foreign key constraint
    addAggregationIdsDbio.flatMap(_ => addSendersDbIO)
  }

  override def pruneExpiredInFlightAggregations(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    // It's enough to delete from `in_flight_aggregation` because deletion cascades to `in_flight_aggregated_sender`
    storage.update_(
      sqlu"delete from seq_in_flight_aggregation where max_sequencing_time <= $upToInclusive",
      functionFullName,
    )
}

object DbSequencerStateManagerStore {
  private final case class AggregatedSignaturesOfSender(signaturesByEnvelope: Seq[Seq[Signature]])(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        AggregatedSignaturesOfSender.type
      ]
  ) extends HasProtocolVersionedWrapper[AggregatedSignaturesOfSender] {
    @transient override protected lazy val companionObj: AggregatedSignaturesOfSender.type =
      AggregatedSignaturesOfSender

    private def toProtoV30: v30.AggregatedSignaturesOfSender =
      v30.AggregatedSignaturesOfSender(
        signaturesByEnvelope = signaturesByEnvelope.map(sigs =>
          v30.AggregatedSignaturesOfSender.SignaturesForEnvelope(sigs.map(_.toProtoV30))
        )
      )
  }

  private object AggregatedSignaturesOfSender
      extends HasProtocolVersionedCompanion[AggregatedSignaturesOfSender]
      with ProtocolVersionedCompanionDbHelpers[AggregatedSignaturesOfSender] {
    override def name: String = "AggregatedSignaturesOfSender"

    override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
      ProtoVersion(30) -> VersionedProtoConverter.storage(
        ReleaseProtocolVersion(ProtocolVersion.v32),
        v30.AggregatedSignaturesOfSender,
      )(
        supportedProtoVersion(_)(fromProtoV30),
        _.toProtoV30.toByteString,
      )
    )

    private def fromProtoV30(
        proto: v30.AggregatedSignaturesOfSender
    ): ParsingResult[AggregatedSignaturesOfSender] = {
      val v30.AggregatedSignaturesOfSender(sigsP) = proto
      for {
        sigs <- sigsP.traverse {
          case v30.AggregatedSignaturesOfSender.SignaturesForEnvelope(sigsForEnvelope) =>
            sigsForEnvelope.traverse(Signature.fromProtoV30)
        }
        rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      } yield AggregatedSignaturesOfSender(sigs)(rpv)
    }
  }
}
