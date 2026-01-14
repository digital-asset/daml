// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.*
import anorm.{Row, RowParser, SimpleSql, ~}
import cats.syntax.all.*
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.backend.Conversions.{
  contractId,
  parties,
  timestampFromMicros,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.*
import com.digitalasset.canton.platform.store.backend.RowDef.*
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy.withoutNetworkTimeout
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*
import com.digitalasset.canton.platform.store.backend.{
  Conversions,
  EventStorageBackend,
  PersistentEventType,
  RowDef,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.PaginationInput
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{ContractId, Party}
import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  FullIdentifier,
  Identifier,
  NameTypeConRefConverter,
}
import com.digitalasset.daml.lf.data.Time.Timestamp

import java.sql.{Connection, PreparedStatement}
import scala.util.Using

object EventStorageBackendTemplate {

  private val MaxBatchSizeOfIncompleteReassignmentOffsetTempTablePopulation: Int = 500

  object RowDefs {
    import CommonRowDefs.*

    // update related
    val workflowId: RowDef[Option[String]] =
      column("workflow_id", str(_).?)

    def sourceSynchronizerId(stringInterning: StringInterning): RowDef[SynchronizerId] =
      genSynchronizerId("source_synchronizer_id")(stringInterning)

    def targetSynchronizerId(stringInterning: StringInterning): RowDef[SynchronizerId] =
      genSynchronizerId("target_synchronizer_id")(stringInterning)

    val eventOffset: RowDef[Offset] =
      offset("event_offset")
    val updateIdDef: RowDef[String] =
      updateId.map(_.toHexString)

    def commandId(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[Option[String]] =
      (
        CommonRowDefs.commandId.?,
        submitters(stringInterning).?,
      ).mapN(filteredCommandId(_, _, allQueryingPartiesO))

    val externalTransactionHash: RowDef[Option[Array[Byte]]] =
      column("external_transaction_hash", byteArray(_).?)

    // event related
    val nodeId: RowDef[Int] =
      column("node_id", int)
    val eventSequentialId: RowDef[Long] =
      column("event_sequential_id", long)

    def filteredAdditionalWitnesses(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    )(witnessIsAcsDelta: Boolean): RowDef[Set[String]] =
      if (witnessIsAcsDelta)
        static(Set.empty)
      else
        column("additional_witnesses", parties(stringInterning)(_))
          .map(filterWitnesses(allQueryingPartiesO, _))

    val eventType: RowDef[PersistentEventType] =
      column("event_type", int(_).map(PersistentEventType.fromInt))
    val deactivatedEventSeqId: RowDef[Option[Long]] =
      column("deactivated_event_sequential_id", long(_).?)

    // contract related
    def representativePackageId(stringInterning: StringInterning): RowDef[Ref.PackageId] =
      column("representative_package_id", int(_).map(stringInterning.packageId.externalize))

    val contractIdDef: RowDef[ContractId] =
      column("contract_id", contractId)
    val internalContractId: RowDef[Long] =
      column("internal_contract_id", long)
    val reassignmentCounter: RowDef[Long] =
      column("reassignment_counter", long(_).?.map(_.getOrElse(0L)))
    val ledgerEffectiveTime: RowDef[Timestamp] =
      column("ledger_effective_time", timestampFromMicros)

    def templateId(stringInterning: StringInterning): RowDef[FullIdentifier] =
      (
        column("template_id", int(_).map(stringInterning.templateId.externalize)),
        column("package_id", int(_).map(stringInterning.packageId.externalize)),
      ).mapN(_ toFullIdentifier _)

    def filteredStakeholderParties(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[Set[String]] =
      // stakeholders are not present in various_witnessed, but exercises and transient/divulged contracts retrieved from there
      column("stakeholders", parties(stringInterning)(_).?)
        .map(_.getOrElse(Seq.empty))
        .map(filterWitnesses(allQueryingPartiesO, _))

    // reassignment related
    val reassignmentId: RowDef[String] =
      column(
        "reassignment_id",
        byteArray(_).map(ReassignmentId.assertFromBytes(_).toProtoPrimitive),
      )

    def submitter(stringInterning: StringInterning): RowDef[Option[String]] =
      column("submitters", parties(stringInterning)(_).?.map(_.getOrElse(Seq.empty).headOption))

    val assignmentExclusivity: RowDef[Option[Timestamp]] =
      column("assignment_exclusivity", timestampFromMicros(_).?)

    // exercise related
    val consuming: RowDef[Boolean] =
      column("consuming", bool(_))

    def exerciseChoice(stringInterning: StringInterning): RowDef[ChoiceName] =
      column("exercise_choice", int(_).map(stringInterning.choiceName.externalize))

    def exerciseChoiceInterface(stringInterning: StringInterning): RowDef[Option[Identifier]] =
      column(
        "exercise_choice_interface",
        int(_).?.map(_.map(stringInterning.interfaceId.externalize)),
      )

    val exerciseArgument: RowDef[Array[Byte]] =
      column("exercise_argument", byteArray(_))
    val exerciseArgumentCompression: RowDef[Option[Int]] =
      column("exercise_argument_compression", int(_).?)
    val exerciseResult: RowDef[Option[Array[Byte]]] =
      column("exercise_result", byteArray(_).?)
    val exerciseResultCompression: RowDef[Option[Int]] =
      column("exercise_result_compression", int(_).?)

    def exerciseActors(stringInterning: StringInterning): RowDef[Set[String]] =
      column("exercise_actors", parties(stringInterning)(_).map(_.map(_.toString).toSet))

    val exerciseLastDescendantNodeId: RowDef[Int] =
      column("exercise_last_descendant_node_id", int)

    // party related
    def partyId(stringInterning: StringInterning) =
      column("party_id", int).map(stringInterning.party.externalize)

    // participant related
    def participantId(stringInterning: StringInterning) =
      column("participant_id", int).map(stringInterning.participantId.externalize)

    // properties
    def commonEventPropertiesParser(
        stringInterning: StringInterning
    ): RowDef[CommonEventProperties] =
      (
        eventSequentialId,
        eventOffset.map(_.unwrap),
        nodeId,
        workflowId,
        synchronizerId(stringInterning).map(_.toProtoPrimitive),
      ).mapN(CommonEventProperties.apply)

    def commonUpdatePropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[CommonUpdateProperties] =
      (
        updateIdDef,
        commandId(stringInterning, allQueryingPartiesO),
        traceContext,
        recordTime,
      ).mapN(CommonUpdateProperties.apply)

    def transactionPropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[TransactionProperties] =
      (
        commonEventPropertiesParser(stringInterning),
        commonUpdatePropertiesParser(stringInterning, allQueryingPartiesO),
        externalTransactionHash,
      ).mapN(TransactionProperties.apply)

    def reassignmentPropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[ReassignmentProperties] =
      (
        commonEventPropertiesParser(stringInterning),
        commonUpdatePropertiesParser(stringInterning, allQueryingPartiesO),
        reassignmentId,
        submitter(stringInterning),
        reassignmentCounter,
      ).mapN(ReassignmentProperties.apply)

    def thinCreatedEventPropertiesParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        witnessIsAcsDelta: Boolean,
        eventIsAcsDeltaForParticipant: Boolean,
    ): RowDef[ThinCreatedEventProperties] =
      (
        representativePackageId(stringInterning),
        filteredAdditionalWitnesses(stringInterning, allQueryingPartiesO)(witnessIsAcsDelta),
        internalContractId,
        static(allQueryingPartiesO.map(_.map(_.toString))),
        if (eventIsAcsDeltaForParticipant) reassignmentCounter else static(0L),
        static(eventIsAcsDeltaForParticipant),
      ).mapN(ThinCreatedEventProperties.apply)

    // raws
    def rawThinActiveContractParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[RawThinActiveContract] =
      (
        commonEventPropertiesParser(stringInterning),
        thinCreatedEventPropertiesParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = allQueryingPartiesO,
          witnessIsAcsDelta = true,
          eventIsAcsDeltaForParticipant = true,
        ),
      ).mapN(RawThinActiveContract.apply)

    def rawThinCreatedEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        witnessIsAcsDelta: Boolean,
        eventIsAcsDeltaForParticipant: Boolean,
    ): RowDef[RawThinCreatedEvent] =
      (
        transactionPropertiesParser(stringInterning, allQueryingPartiesO),
        thinCreatedEventPropertiesParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = allQueryingPartiesO,
          witnessIsAcsDelta = witnessIsAcsDelta,
          eventIsAcsDeltaForParticipant = eventIsAcsDeltaForParticipant,
        ),
      ).mapN(RawThinCreatedEvent.apply)

    def rawThinAssignEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[RawThinAssignEvent] =
      (
        reassignmentPropertiesParser(stringInterning, allQueryingPartiesO),
        thinCreatedEventPropertiesParser(
          stringInterning = stringInterning,
          allQueryingPartiesO = allQueryingPartiesO,
          witnessIsAcsDelta = true,
          eventIsAcsDeltaForParticipant = true,
        ),
        sourceSynchronizerId(stringInterning).map(_.toProtoPrimitive),
      ).mapN(RawThinAssignEvent.apply)

    def rawArchivedEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        acsDeltaForParticipant: Boolean,
    ): RowDef[RawArchivedEvent] =
      (
        transactionPropertiesParser(stringInterning, allQueryingPartiesO),
        contractIdDef,
        templateId(stringInterning),
        if (acsDeltaForParticipant) filteredStakeholderParties(stringInterning, allQueryingPartiesO)
        else static(Set.empty[String]),
        ledgerEffectiveTime,
        if (acsDeltaForParticipant) deactivatedEventSeqId
        else static(None),
      ).mapN(RawArchivedEvent.apply)

    def rawExercisedEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
        eventIsAcsDeltaForParticipant: Boolean,
    ): RowDef[RawExercisedEvent] =
      (
        transactionPropertiesParser(stringInterning, allQueryingPartiesO),
        contractIdDef,
        templateId(stringInterning),
        if (eventIsAcsDeltaForParticipant) static(true) else consuming,
        exerciseChoice(stringInterning),
        exerciseChoiceInterface(stringInterning),
        exerciseArgument,
        exerciseArgumentCompression,
        exerciseResult,
        exerciseResultCompression,
        exerciseActors(stringInterning),
        exerciseLastDescendantNodeId,
        filteredAdditionalWitnesses(stringInterning, allQueryingPartiesO)(witnessIsAcsDelta =
          false
        ),
        if (eventIsAcsDeltaForParticipant)
          filteredStakeholderParties(stringInterning, allQueryingPartiesO)
        else static(Set.empty[String]),
        ledgerEffectiveTime,
        if (eventIsAcsDeltaForParticipant) deactivatedEventSeqId
        else static(Option.empty[Long]),
        static(eventIsAcsDeltaForParticipant),
      ).mapN(RawExercisedEvent.apply)

    def rawUnassignEventParser(
        stringInterning: StringInterning,
        allQueryingPartiesO: Option[Set[Party]],
    ): RowDef[RawUnassignEvent] =
      (
        reassignmentPropertiesParser(stringInterning, allQueryingPartiesO),
        contractIdDef,
        templateId(stringInterning),
        filteredStakeholderParties(stringInterning, allQueryingPartiesO),
        assignmentExclusivity,
        targetSynchronizerId(stringInterning).map(_.toProtoPrimitive),
        deactivatedEventSeqId,
      ).mapN(RawUnassignEvent.apply)

    private def authorizationEventParser(
        authorizationLevelColumnName: String,
        authorizationEventTypeColumnName: String,
    ): RowDef[AuthorizationEvent] =
      (
        column(authorizationEventTypeColumnName, int),
        column(authorizationLevelColumnName, int),
      ).mapN(Conversions.authorizationEvent)

    def partyToParticipantEventParser(
        stringInterning: StringInterning
    ): RowDef[RawParticipantAuthorization] =
      (
        eventOffset,
        updateIdDef,
        partyId(stringInterning),
        participantId(stringInterning),
        authorizationEventParser("participant_permission", "participant_authorization_event"),
        recordTime,
        synchronizerId(stringInterning).map(_.toProtoPrimitive),
        traceContext.?,
      ).mapN(
        RawParticipantAuthorization.apply
      )

    private def synchronizerOffsetParser(
        offsetColumnName: String,
        stringInterning: StringInterning,
    ): RowDef[SynchronizerOffset] =
      (
        offset(offsetColumnName),
        synchronizerId(stringInterning),
        recordTime,
        publicationTime,
      ).mapN(SynchronizerOffset.apply)

    def completionSynchronizerOffsetParser(
        stringInterning: StringInterning
    ): RowDef[SynchronizerOffset] =
      synchronizerOffsetParser("completion_offset", stringInterning)

    def metaSynchronizerOffsetParser(
        stringInterning: StringInterning
    ): RowDef[SynchronizerOffset] =
      synchronizerOffsetParser("event_offset", stringInterning)
  }

  val EventSequentialIdFirstLast: RowParser[(Long, Long)] =
    long("event_sequential_id_first") ~ long("event_sequential_id_last") map {
      case event_sequential_id_first ~ event_sequential_id_last =>
        (event_sequential_id_first, event_sequential_id_last)
    }

  private def filterWitnesses(
      allQueryingPartiesO: Option[Set[Party]],
      witnesses: Seq[Party],
  ): Set[String] =
    allQueryingPartiesO
      .fold(witnesses)(allQueryingParties =>
        witnesses
          .filter(allQueryingParties)
      )
      .toSet

  private def filteredCommandId(
      commandId: Option[String],
      submitters: Option[Seq[Party]],
      allQueryingPartiesO: Option[Set[Party]],
  ): Option[String] = {
    def submittersInQueryingParties: Boolean = allQueryingPartiesO match {
      case Some(allQueryingParties) =>
        submitters.getOrElse(Seq.empty).exists(allQueryingParties)
      case None => submitters.nonEmpty
    }
    commandId.filter(_ != "" && submittersInQueryingParties)
  }
}

abstract class EventStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    val loggerFactory: NamedLoggerFactory,
) extends EventStorageBackend
    with NamedLogging {
  import EventStorageBackendTemplate.*
  import com.digitalasset.canton.platform.store.backend.Conversions.OffsetToStatement

  override def updatePointwiseQueries: UpdatePointwiseQueries =
    new UpdatePointwiseQueries(ledgerEndCache)

  override def updateStreamingQueries: UpdateStreamingQueries =
    new UpdateStreamingQueries(stringInterning, queryStrategy)

  override def eventReaderQueries: EventReaderQueries =
    new EventReaderQueries(stringInterning)

  override def pruneEvents(
      previousPruneUpToInclusiveOffset: Option[Offset],
      previousIncompleteReassignmentOffsets: Vector[Offset],
      pruneUpToInclusiveOffset: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit connection: Connection, traceContext: TraceContext): Unit =
    // pruning events could be a long-running operation, so we disable the network timeout
    withoutNetworkTimeout { implicit connection =>
      /*
      Incomplete assign: we need to retain the next deactivation if any (otherwise it becomes active)
      Incomplete unassign: we need to retain the previous activation (otherwise it cannot be presented)
      Deactivation: we need to prune the related activation (otherwise it becomes active)
      Pruning always happens in pairs, never ever we prune only activation or deactivation under normal circumstances.
      Only exception to this rule is when the deactivation reference was unable to be calculated: then we can prune this orphan deactivation.
      Therefore, regarding activations and deactivations: we prune based on the identified deactivations.
      We prune all witnessed events (cannot be active, cannot be incomplete).
       */

      def loadOffsets(
          offsets: Vector[Offset],
          purpose: String,
      )(jdbcQueryString: String)(setParams: PreparedStatement => Offset => Unit): Unit =
        if (offsets.nonEmpty) {
          Using.resource(
            connection.prepareStatement(jdbcQueryString)
          ) { preparedStatement =>
            val offsetBatches = offsets
              .grouped(MaxBatchSizeOfIncompleteReassignmentOffsetTempTablePopulation)
              .toVector
            logger.info(
              s"Loading ${offsets.size} offsets in ${offsetBatches.size} batches for $purpose"
            )
            offsetBatches.iterator.zipWithIndex
              .foreach { case (batch, index) =>
                batch.foreach { offset =>
                  setParams(preparedStatement)(offset)
                  preparedStatement.addBatch()
                }
                preparedStatement.executeBatch().discard
                logger.debug(
                  s"Processed offset batch #${index + 1} / ${offsetBatches.size} for $purpose"
                )
              }
            logger.info(
              s"Loaded ${offsets.size} offsets in ${offsetBatches.size} batches for $purpose"
            )
          }
        }

      val pruningFromExclusiveEventSeqId =
        maxEventSequentialId(previousPruneUpToInclusiveOffset)(connection)
      val pruningToInclusiveEventSeqId =
        maxEventSequentialId(Some(pruneUpToInclusiveOffset))(connection)

      logger.info(
        s"Start pruning Index DB events. Offsets in range ($previousPruneUpToInclusiveOffset, $pruneUpToInclusiveOffset] event sequential IDs in range ($pruningFromExclusiveEventSeqId, $pruningToInclusiveEventSeqId] with ${previousIncompleteReassignmentOffsets.size} incomplete offsets at the beginning and with ${incompleteReassignmentOffsets.size} at the end of the pruning range."
      )

      val currentIncompleteSet = incompleteReassignmentOffsets.toSet
      val completedReassignments =
        previousIncompleteReassignmentOffsets.filterNot(currentIncompleteSet)

      logger.info("Creating temporary table for storing pruning candidates")
      SQL"""
      -- Create temporary table for storing deactivated_contract candidates for pruning
      CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS temp_candidate_deactivated (
        deactivate_event_sequential_id bigint NOT NULL,
        activate_event_sequential_id bigint
      ) ON COMMIT DELETE ROWS
      """.execute().discard

      // Please note: the big union + join query is deliberately pu in one CTE due to some H2 bug.
      // (possibly the H2 bug is around multiple CTEs targeting the same table)
      loadOffsets(completedReassignments, "populating candidates for completed reassignments")(
        """-- first unfolding an offset to all respective event_sequential_id-s
        |WITH completed_ids AS (
        |  -- completed incomplete unassign: we carry the respective activation as well, both could be pruned now
        |  SELECT
        |    d1.event_sequential_id,
        |    d1.deactivated_event_sequential_id
        |  FROM lapi_events_deactivate_contract d1 WHERE event_offset = ?
        |  UNION ALL
        |  SELECT
        |    d2.event_sequential_id as event_sequential_id,
        |    d2.deactivated_event_sequential_id as deactivated_event_sequential_id
        |  FROM lapi_events_deactivate_contract d2, lapi_events_activate_contract a
        |  WHERE
        |    a.event_offset = ?
        |    -- completed incomplete assign: we only prune if the deactivation is found and is below the fromExclusive
        |    -- because the deactivations above will anyway added to the candidates
        |    AND d2.deactivated_event_sequential_id = a.event_sequential_id
        |    AND d2.event_sequential_id <= ?
        |)
        |INSERT INTO temp_candidate_deactivated(deactivate_event_sequential_id, activate_event_sequential_id)
        |SELECT event_sequential_id, deactivated_event_sequential_id
        |FROM completed_ids""".stripMargin
      ) { preparedStatement => offset =>
        preparedStatement.setLong(1, offset.unwrap)
        preparedStatement.setLong(2, offset.unwrap)
        preparedStatement.setLong(3, pruningFromExclusiveEventSeqId)
      }

      logger.info("Populating candidates for deactivated contracts in the pruned range")
      SQL"""
      -- Create temporary table for storing deactivated_contract candidates for pruning
      INSERT INTO temp_candidate_deactivated(deactivate_event_sequential_id, activate_event_sequential_id)
      SELECT event_sequential_id, deactivated_event_sequential_id
      FROM lapi_events_deactivate_contract
      WHERE
        event_sequential_id > $pruningFromExclusiveEventSeqId
        AND event_sequential_id <= $pruningToInclusiveEventSeqId
      """.execute().discard

      logger.info("Indexing temporary table for storing pruning candidates")
      SQL"""
      -- Create temporary table for storing deactivated_contract candidates for pruning
      CREATE INDEX temp_candidate_deactivated_deactivate ON temp_candidate_deactivated USING btree (deactivate_event_sequential_id);
      CREATE INDEX temp_candidate_deactivated_activate ON temp_candidate_deactivated USING btree (activate_event_sequential_id);
      ${queryStrategy.analyzeTable("temp_candidate_deactivated")};
      """.execute().discard

      loadOffsets(
        incompleteReassignmentOffsets,
        "removing candidates related to incomplete reassignments",
      )(
        """-- first unfolding an offset to all respective event_sequential_id-s
        |WITH incomplete_ids AS (
        |  SELECT event_sequential_id FROM lapi_events_activate_contract WHERE event_offset = ?
        |  UNION ALL
        |  SELECT event_sequential_id FROM lapi_events_deactivate_contract WHERE event_offset = ?
        |)
        |DELETE FROM temp_candidate_deactivated
        |WHERE EXISTS (
        |  SELECT 1
        |  FROM incomplete_ids
        |  WHERE
        |    -- either respective activation or deactivation is a match, we need to remove both - the whole row
        |    incomplete_ids.event_sequential_id = temp_candidate_deactivated.deactivate_event_sequential_id
        |    OR incomplete_ids.event_sequential_id = temp_candidate_deactivated.activate_event_sequential_id
        |)""".stripMargin
      ) { preparedStatement => offset =>
        preparedStatement.setLong(1, offset.unwrap)
        preparedStatement.setLong(2, offset.unwrap)
      }

      // activate
      pruneWithLogging("Pruning lapi_events_activate_contract table") {
        SQL"""
        DELETE from lapi_events_activate_contract
        WHERE EXISTS (
          SELECT 1
          FROM temp_candidate_deactivated
          WHERE lapi_events_activate_contract.event_sequential_id = temp_candidate_deactivated.activate_event_sequential_id
        )"""
      }
      pruneWithLogging("Pruning lapi_filter_activate_stakeholder table") {
        SQL"""
        DELETE from lapi_filter_activate_stakeholder
        WHERE EXISTS (
          SELECT 1
          FROM temp_candidate_deactivated
          WHERE lapi_filter_activate_stakeholder.event_sequential_id = temp_candidate_deactivated.activate_event_sequential_id
        )"""
      }
      pruneWithLogging("Pruning lapi_filter_activate_witness table") {
        SQL"""
        DELETE from lapi_filter_activate_witness
        WHERE EXISTS (
          SELECT 1
          FROM temp_candidate_deactivated
          WHERE lapi_filter_activate_witness.event_sequential_id = temp_candidate_deactivated.activate_event_sequential_id
        )"""
      }

      // deactivate
      pruneWithLogging("Pruning lapi_events_deactivate_contract table") {
        SQL"""
        DELETE from lapi_events_deactivate_contract
        WHERE EXISTS (
          SELECT 1
          FROM temp_candidate_deactivated
          WHERE lapi_events_deactivate_contract.event_sequential_id = temp_candidate_deactivated.deactivate_event_sequential_id
        )"""
      }
      pruneWithLogging("Pruning lapi_filter_deactivate_stakeholder table") {
        SQL"""
        DELETE from lapi_filter_deactivate_stakeholder
        WHERE EXISTS (
          SELECT 1
          FROM temp_candidate_deactivated
          WHERE lapi_filter_deactivate_stakeholder.event_sequential_id = temp_candidate_deactivated.deactivate_event_sequential_id
        )"""
      }
      pruneWithLogging("Pruning lapi_filter_deactivate_witness table") {
        SQL"""
        DELETE from lapi_filter_deactivate_witness
        WHERE EXISTS (
          SELECT 1
          FROM temp_candidate_deactivated
          WHERE lapi_filter_deactivate_witness.event_sequential_id = temp_candidate_deactivated.deactivate_event_sequential_id
        )"""
      }

      logger.info("Dropping temporary table for storing pruning candidates")
      SQL"""
      DROP INDEX temp_candidate_deactivated_deactivate;
      DROP INDEX temp_candidate_deactivated_activate;
      DROP TABLE temp_candidate_deactivated;
      """.execute().discard

      // witnessed
      pruneWithLogging("Pruning lapi_events_various_witnessed table") {
        SQL"""
        DELETE from lapi_events_various_witnessed
        WHERE
          event_sequential_id <= $pruningToInclusiveEventSeqId AND
          event_sequential_id > $pruningFromExclusiveEventSeqId"""
      }
      pruneWithLogging("Pruning lapi_filter_various_witness table") {
        SQL"""
        DELETE from lapi_filter_various_witness
        WHERE
          event_sequential_id <= $pruningToInclusiveEventSeqId AND
          event_sequential_id > $pruningFromExclusiveEventSeqId"""
      }

      // meta
      pruneWithLogging("Pruning lapi_update_meta table") {
        SQL"""
        DELETE FROM lapi_update_meta
        WHERE
          event_offset <= $pruneUpToInclusiveOffset AND
          ${QueryStrategy.offsetIsGreater("event_offset", previousPruneUpToInclusiveOffset)}"""
      }

      logger.info("Finished pruning of Index DB events.")
    }(connection, logger)

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(implicit
      connection: Connection,
      traceContext: TraceContext,
  ): Unit = {
    logger.info(s"$queryDescription")
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")
  }

  override def maxEventSequentialId(
      untilInclusiveOffset: Option[Offset]
  )(connection: Connection): Long = {
    val ledgerEnd = ledgerEndCache()
    SQL"""
     SELECT
        event_sequential_id_first
     FROM
        lapi_update_meta
     WHERE
        ${QueryStrategy.offsetIsGreater("event_offset", untilInclusiveOffset)}
        AND ${QueryStrategy.offsetIsLessOrEqual("event_offset", ledgerEnd.map(_.lastOffset))}
     ORDER BY
        event_offset
     ${QueryStrategy.limitClause(Some(1))}
   """.as(get[Long](1).singleOpt)(connection)
      .getOrElse(
        // after the offset there is no meta, so no tx,
        // therefore the next (minimum) event sequential id will be
        // the first event sequential id after the ledger end
        ledgerEnd.map(_.lastEventSeqId).getOrElse(0L) + 1
      ) - 1
  }

  override def activeContractBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Option[Set[Party]],
  )(connection: Connection): Vector[RawThinActiveContract] =
    RowDefs
      .rawThinActiveContractParser(stringInterning, allFilterParties)
      .queryMultipleRows(columns =>
        SQL"""
        SELECT $columns
        FROM lapi_events_activate_contract
        WHERE
          event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        ORDER BY event_sequential_id -- deliver in index order
        """
          .withFetchSize(Some(eventSequentialIds.size))
      )(connection)

  override def lookupActivationSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM lapi_events_activate_contract
        WHERE
          event_offset ${queryStrategy.anyOf(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def lookupDeactivationSequentialIdByOffset(
      offsets: Iterable[Long]
  )(connection: Connection): Vector[Long] =
    SQL"""
        SELECT event_sequential_id
        FROM lapi_events_deactivate_contract
        WHERE
          event_offset ${queryStrategy.anyOf(offsets)}
        ORDER BY event_sequential_id -- deliver in index order
        """
      .asVectorOf(long("event_sequential_id"))(connection)

  override def firstSynchronizerOffsetAfterOrAt(
      synchronizerId: SynchronizerId,
      afterOrAtRecordTimeInclusive: Timestamp,
  )(connection: Connection): Option[SynchronizerOffset] =
    List(
      RowDefs
        .completionSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_command_completions
          WHERE
            synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
            record_time >= ${afterOrAtRecordTimeInclusive.micros}
          ORDER BY synchronizer_id ASC, record_time ASC, completion_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
      RowDefs
        .metaSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_update_meta
          WHERE
            synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
            record_time >= ${afterOrAtRecordTimeInclusive.micros}
          ORDER BY synchronizer_id ASC, record_time ASC, event_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
    ).flatten
      .minByOption(_.recordTime)
      .filter(synchronizerOffset =>
        Option(synchronizerOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // if the first is after LedgerEnd, then we have none

  override def lastSynchronizerOffsetBeforeOrAt(
      synchronizerIdO: Option[SynchronizerId],
      beforeOrAtOffsetInclusive: Offset,
  )(connection: Connection): Option[SynchronizerOffset] = {
    val ledgerEndOffset = ledgerEndCache().map(_.lastOffset)
    val safeBeforeOrAtOffset =
      if (Option(beforeOrAtOffsetInclusive) > ledgerEndOffset) ledgerEndOffset
      else Some(beforeOrAtOffsetInclusive)
    val (synchronizerIdFilter, synchronizerIdOrdering) = synchronizerIdO match {
      case Some(synchronizerId) =>
        (
          cSQL"synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND",
          cSQL"synchronizer_id,",
        )

      case None =>
        (
          cSQL"",
          cSQL"",
        )
    }
    List(
      RowDefs
        .completionSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_command_completions
          WHERE
            $synchronizerIdFilter
            ${QueryStrategy.offsetIsLessOrEqual("completion_offset", safeBeforeOrAtOffset)}
          ORDER BY $synchronizerIdOrdering completion_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
      RowDefs
        .metaSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_update_meta
          WHERE
            $synchronizerIdFilter
            ${QueryStrategy.offsetIsLessOrEqual("event_offset", safeBeforeOrAtOffset)}
          ORDER BY $synchronizerIdOrdering event_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
    ).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  // Note: Added for offline party replication as CN is using it.
  override def lastSynchronizerOffsetBeforeOrAtRecordTime(
      synchronizerId: SynchronizerId,
      beforeOrAtRecordTimeInclusive: Timestamp,
  )(connection: Connection)(implicit traceContext: TraceContext): Option[SynchronizerOffset] = {
    val ledgerEndOffset = ledgerEndCache().map(_.lastOffset)

    logger.debug(
      s"Querying lastSynchronizerOffset: beforeOrAtRecordTime=$beforeOrAtRecordTimeInclusive, ledgerEndOffset=$ledgerEndOffset, synchronizerId=$synchronizerId"
    )

    val completionQueryResult =
      RowDefs
        .completionSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
        SELECT $columns
        FROM lapi_command_completions
        WHERE
          synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
          record_time <= ${beforeOrAtRecordTimeInclusive.micros} AND
          ${QueryStrategy.offsetIsLessOrEqual("completion_offset", ledgerEndOffset)}
        ORDER BY synchronizer_id DESC, record_time DESC, completion_offset DESC
        ${QueryStrategy.limitClause(Some(1))}
        """)(connection)

    logger.debug(s"lapi_command_completions query result: $completionQueryResult")

    val metaQueryResult =
      RowDefs
        .metaSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
        SELECT $columns
        FROM lapi_update_meta
        WHERE
          synchronizer_id = ${stringInterning.synchronizerId.internalize(synchronizerId)} AND
          record_time <= ${beforeOrAtRecordTimeInclusive.micros} AND
          ${QueryStrategy.offsetIsLessOrEqual("event_offset", ledgerEndOffset)}
        ORDER BY synchronizer_id DESC, record_time DESC, event_offset DESC
        ${QueryStrategy.limitClause(Some(1))}
        """)(connection)

    logger.debug(s"lapi_update_meta query result: $metaQueryResult")

    List(completionQueryResult, metaQueryResult).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  override def synchronizerOffset(offset: Offset)(
      connection: Connection
  ): Option[SynchronizerOffset] =
    List(
      RowDefs
        .completionSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_command_completions
          WHERE
            completion_offset = $offset
          """)(connection),
      RowDefs
        .metaSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_update_meta
          WHERE
            event_offset = $offset
          """)(connection),
    ).flatten.headOption // if both present they should be the same
      .filter(synchronizerOffset =>
        Option(synchronizerOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // only offset allow before or at ledger end

  override def firstSynchronizerOffsetAfterOrAtPublicationTime(
      afterOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[SynchronizerOffset] =
    List(
      RowDefs
        .completionSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_command_completions
          WHERE
            publication_time >= ${afterOrAtPublicationTimeInclusive.micros}
          ORDER BY publication_time ASC, completion_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
      RowDefs
        .metaSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_update_meta
          WHERE
            publication_time >= ${afterOrAtPublicationTimeInclusive.micros}
          ORDER BY publication_time ASC, event_offset ASC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
    ).flatten
      .minByOption(_.offset)
      .filter(synchronizerOffset =>
        Option(synchronizerOffset.offset) <= ledgerEndCache().map(_.lastOffset)
      ) // if first offset is beyond the ledger-end then we have no such

  override def lastSynchronizerOffsetBeforeOrAtPublicationTime(
      beforeOrAtPublicationTimeInclusive: Timestamp
  )(connection: Connection): Option[SynchronizerOffset] = {
    val ledgerEndPublicationTime =
      ledgerEndCache().map(_.lastPublicationTime).getOrElse(CantonTimestamp.MinValue).underlying
    val safePublicationTime =
      if (beforeOrAtPublicationTimeInclusive > ledgerEndPublicationTime)
        ledgerEndPublicationTime
      else
        beforeOrAtPublicationTimeInclusive
    List(
      RowDefs
        .completionSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_command_completions
          WHERE
            publication_time <= ${safePublicationTime.micros}
          ORDER BY publication_time DESC, completion_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
      RowDefs
        .metaSynchronizerOffsetParser(stringInterning)
        .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_update_meta
          WHERE
            publication_time <= ${safePublicationTime.micros}
          ORDER BY publication_time DESC, event_offset DESC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection),
    ).flatten
      .sortBy(_.offset)
      .reverse
      .headOption
  }

  override def prunableContracts(fromExclusive: Option[Offset], toInclusive: Offset)(
      connection: Connection
  ): Set[Long] = {
    val fromExclusiveSeqId =
      fromExclusive
        .map(from => maxEventSequentialId(Some(from))(connection))
        .getOrElse(-1L)
    val toInclusiveSeqId = maxEventSequentialId(Some(toInclusive))(connection)
    val archivals = SQL"""
        SELECT internal_contract_id
        FROM lapi_events_deactivate_contract
        WHERE
          event_sequential_id > $fromExclusiveSeqId AND
          event_sequential_id <= $toInclusiveSeqId AND
          event_type = ${PersistentEventType.ConsumingExercise.asInt}
        """
      .asVectorOf(long("internal_contract_id").?)(connection)
    val divulgedAndTransientContracts = SQL"""
        SELECT internal_contract_id
        FROM lapi_events_various_witnessed
        WHERE
          event_sequential_id > $fromExclusiveSeqId AND
          event_sequential_id <= $toInclusiveSeqId AND
          event_type = ${PersistentEventType.WitnessedCreate.asInt}
        """
      .asVectorOf(long("internal_contract_id").?)(connection)
    archivals.iterator
      .++(divulgedAndTransientContracts.iterator)
      .flatten
      .toSet
  }

  override def fetchTopologyPartyEventIds(
      party: Option[Party]
  )(connection: Connection): PaginationInput => Vector[Long] =
    UpdateStreamingQueries.fetchEventIds(
      tableName = "lapi_events_party_to_participant",
      witnessO = party,
      templateIdO = None,
      idFilter = None,
      stringInterning = stringInterning,
      hasFirstPerSequentialId = false,
    )(connection)

  override def topologyPartyEventBatch(
      eventSequentialIds: SequentialIdBatch
  )(connection: Connection): Vector[EventStorageBackend.RawParticipantAuthorization] = {
    val query = (columns: CompositeSql) =>
      SQL"""
          SELECT $columns
          FROM lapi_events_party_to_participant e
          WHERE ${queryStrategy.inBatch("e.event_sequential_id", eventSequentialIds)}
          ORDER BY e.event_sequential_id -- deliver in index order
          """
        .withFetchSize(Some(fetchSize(eventSequentialIds)))
    RowDefs.partyToParticipantEventParser(stringInterning).queryMultipleRows(query)(connection)
  }

  override def topologyEventOffsetPublishedOnRecordTime(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(connection: Connection): Option[Offset] =
    stringInterning.synchronizerId
      .tryInternalize(synchronizerId)
      .flatMap(synchronizerInternedId =>
        RowDefs.eventOffset
          .querySingleOptRow(columns => SQL"""
          SELECT $columns
          FROM lapi_events_party_to_participant
          WHERE record_time = ${recordTime.toMicros}
                AND synchronizer_id = $synchronizerInternedId
          ORDER BY synchronizer_id ASC, record_time ASC
          ${QueryStrategy.limitClause(Some(1))}
          """)(connection)
          .filter(offset => Option(offset) <= ledgerEndCache().map(_.lastOffset))
      )

  private def fetchByEventSequentialIds(
      tableName: String,
      eventSequentialIds: SequentialIdBatch,
  )(columns: CompositeSql): SimpleSql[Row] =
    SQL"""
        SELECT $columns
        FROM #$tableName
        WHERE ${queryStrategy.inBatch("event_sequential_id", eventSequentialIds)}
        ORDER BY event_sequential_id
        """.withFetchSize(Some(fetchSize(eventSequentialIds)))

  override def fetchEventPayloadsAcsDelta(target: EventPayloadSourceForUpdatesAcsDelta)(
      eventSequentialIds: SequentialIdBatch,
      requestingPartiesForTx: Option[Set[Party]],
      requestingPartiesForReassignment: Option[Set[Party]],
  )(connection: Connection): Vector[RawThinAcsDeltaEvent] =
    target match {
      case EventPayloadSourceForUpdatesAcsDelta.Activate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.Create -> RowDefs.rawThinCreatedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForTx,
              witnessIsAcsDelta = true,
              eventIsAcsDeltaForParticipant = true,
            ),
            PersistentEventType.Assign -> RowDefs.rawThinAssignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForReassignment,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_activate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
      case EventPayloadSourceForUpdatesAcsDelta.Deactivate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.ConsumingExercise -> RowDefs.rawArchivedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForTx,
              acsDeltaForParticipant = true,
            ),
            PersistentEventType.Unassign -> RowDefs.rawUnassignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForReassignment,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_deactivate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
    }

  override def fetchEventPayloadsLedgerEffects(target: EventPayloadSourceForUpdatesLedgerEffects)(
      eventSequentialIds: SequentialIdBatch,
      requestingPartiesForTx: Option[Set[Party]],
      requestingPartiesForReassignment: Option[Set[Party]],
  )(connection: Connection): Vector[RawThinLedgerEffectsEvent] =
    target match {
      case EventPayloadSourceForUpdatesLedgerEffects.Activate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.Create -> RowDefs.rawThinCreatedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForTx,
              witnessIsAcsDelta = false,
              eventIsAcsDeltaForParticipant = true,
            ),
            PersistentEventType.Assign -> RowDefs.rawThinAssignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForReassignment,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_activate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
      case EventPayloadSourceForUpdatesLedgerEffects.Deactivate =>
        RowDefs.eventType
          .branch(
            PersistentEventType.ConsumingExercise -> RowDefs.rawExercisedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForTx,
              eventIsAcsDeltaForParticipant = true,
            ),
            PersistentEventType.Unassign -> RowDefs.rawUnassignEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForReassignment,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_deactivate_contract",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
      case EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed =>
        RowDefs.eventType
          .branch(
            PersistentEventType.WitnessedCreate -> RowDefs.rawThinCreatedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForTx,
              witnessIsAcsDelta = false,
              eventIsAcsDeltaForParticipant = false,
            ),
            PersistentEventType.WitnessedConsumingExercise -> RowDefs.rawExercisedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForTx,
              eventIsAcsDeltaForParticipant = false,
            ),
            PersistentEventType.NonConsumingExercise -> RowDefs.rawExercisedEventParser(
              stringInterning = stringInterning,
              allQueryingPartiesO = requestingPartiesForTx,
              eventIsAcsDeltaForParticipant = false,
            ),
          )
          .queryMultipleRows(
            fetchByEventSequentialIds(
              tableName = "lapi_events_various_witnessed",
              eventSequentialIds = eventSequentialIds,
            )
          )(connection)
    }

  private def fetchSize(eventSequentialIds: SequentialIdBatch): Int =
    eventSequentialIds match {
      case SequentialIdBatch.IdRange(fromInclusive, toInclusive) =>
        Math.min(toInclusive - fromInclusive + 1, Int.MaxValue).toInt
      case SequentialIdBatch.Ids(ids) => ids.size
    }

}
