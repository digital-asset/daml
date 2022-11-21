// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import anorm.SqlParser.{array, bool, byteArray, get, int, long, str}
import anorm.{Row, RowParser, SimpleSql, ~}
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.Conversions.{
  contractId,
  eventId,
  hashFromHexString,
  offset,
  timestampFromMicros,
}
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.platform.store.dao.events.Raw
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.EventStorageBackend.RawTransactionEvent
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

import scala.collection.immutable.ArraySeq

object EventStorageBackendTemplate {
  import com.daml.platform.store.backend.Conversions.ArrayColumnToIntArray._
  import com.daml.platform.store.backend.Conversions.ArrayColumnToStringArray._

  // TOOD etq: Move non-private members to the top

  private val baseColumnsForFlatTransactionsCreate =
    Seq(
      "event_offset",
      "transaction_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "event_id",
      "contract_id",
      "template_id",
      "create_argument",
      "create_argument_compression",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
      "create_key_hash",
      "create_key_value_compression",
      "submitters",
      "driver_metadata",
    )

  private val baseColumnsForFlatTransactionsExercise =
    Seq(
      "event_offset",
      "transaction_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "event_id",
      "contract_id",
      "template_id",
      "NULL as create_argument",
      "NULL as create_argument_compression",
      "NULL as create_signatories",
      "NULL as create_observers",
      "NULL as create_agreement_text",
      "create_key_value",
      "NULL as create_key_hash",
      "create_key_value_compression",
      "submitters",
      "NULL as driver_metadata",
    )

  val selectColumnsForFlatTransactionsCreate: String =
    baseColumnsForFlatTransactionsCreate.mkString(", ")

  val selectColumnsForFlatTransactionsExercise: String =
    baseColumnsForFlatTransactionsExercise.mkString(", ")

  private val selectColumnsForACSEvents =
    baseColumnsForFlatTransactionsCreate.map(c => s"create_evs.$c").mkString(", ")

  private type SharedRow =
    Offset ~ String ~ Int ~ Long ~ String ~ String ~ Timestamp ~ Int ~ Option[String] ~
      Option[String] ~ Array[Int] ~ Option[Array[Int]]

  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      int("node_index") ~
      long("event_sequential_id") ~
      str("event_id") ~
      str("contract_id") ~
      timestampFromMicros("ledger_effective_time") ~
      int("template_id") ~
      str("command_id").? ~
      str("workflow_id").? ~
      array[Int]("event_witnesses") ~
      array[Int]("submitters").?

  private type CreatedEventRow =
    SharedRow ~ Array[Byte] ~ Option[Int] ~ Array[Int] ~ Array[Int] ~ Option[String] ~
      Option[Array[Byte]] ~ Option[Hash] ~ Option[Int] ~ Option[Array[Byte]]

  private val createdEventRow: RowParser[CreatedEventRow] =
    sharedRow ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      array[Int]("create_signatories") ~
      array[Int]("create_observers") ~
      str("create_agreement_text").? ~
      byteArray("create_key_value").? ~
      hashFromHexString("create_key_hash").? ~
      int("create_key_value_compression").? ~
      byteArray("driver_metadata").?

  private type ExercisedEventRow =
    SharedRow ~ Boolean ~ String ~ Array[Byte] ~ Option[Int] ~ Option[Array[Byte]] ~ Option[Int] ~
      Array[Int] ~ Array[String]

  private val exercisedEventRow: RowParser[ExercisedEventRow] = {
    import com.daml.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    sharedRow ~
      bool("exercise_consuming") ~
      str("exercise_choice") ~
      byteArray("exercise_argument") ~
      int("exercise_argument_compression").? ~
      byteArray("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[Int]("exercise_actors") ~
      array[String]("exercise_child_event_ids")
  }

  private type ArchiveEventRow = SharedRow

  private val archivedEventRow: RowParser[ArchiveEventRow] = sharedRow

  private def createdFlatEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~
          templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ createArgument ~ createArgumentCompression ~
          createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue ~ createKeyHash ~ createKeyValueCompression ~ driverMetadata =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId != "" && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createSignatories = ArraySeq.unsafeWrapArray(
              createSignatories.map(stringInterning.party.unsafe.externalize)
            ),
            createObservers = ArraySeq.unsafeWrapArray(
              createObservers.map(stringInterning.party.unsafe.externalize)
            ),
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            createKeyHash = createKeyHash,
            createKeyValueCompression = createKeyValueCompression,
            ledgerEffectiveTime = ledgerEffectiveTime,
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
            driverMetadata = driverMetadata,
          ),
        )
    }

  private def archivedFlatEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent.Archived]] =
    archivedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId != "" && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Archived(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
          ),
        )
    }

  def rawFlatEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent]] =
    createdFlatEventParser(allQueryingParties, stringInterning) | archivedFlatEventParser(
      allQueryingParties,
      stringInterning,
    )

  private def createdTreeEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ createArgument ~ createArgumentCompression ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue ~ createKeyHash ~ createKeyValueCompression ~ driverMetadata =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId != "" && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.TreeEvent.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createSignatories = ArraySeq.unsafeWrapArray(
              createSignatories.map(stringInterning.party.unsafe.externalize)
            ),
            createObservers = ArraySeq.unsafeWrapArray(
              createObservers.map(stringInterning.party.unsafe.externalize)
            ),
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            createKeyHash = createKeyHash,
            ledgerEffectiveTime = ledgerEffectiveTime,
            createKeyValueCompression = createKeyValueCompression,
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
            driverMetadata = driverMetadata,
          ),
        )
    }

  private def exercisedTreeEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent.Exercised]] =
    exercisedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ exerciseConsuming ~ qualifiedChoiceName ~ exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~ exerciseChildEventIds =>
        val Ref.QualifiedChoiceName(interfaceId, choiceName) =
          Ref.QualifiedChoiceName.assertFromString(qualifiedChoiceName)
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventStorageBackend.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId
            .filter(commandId =>
              commandId.nonEmpty && submitters.getOrElse(Array.empty).exists(allQueryingParties)
            )
            .getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.TreeEvent.Exercised(
            eventId = eventId,
            contractId = contractId,
            templateId = stringInterning.templateId.externalize(templateId),
            interfaceId = interfaceId,
            exerciseConsuming = exerciseConsuming,
            exerciseChoice = choiceName,
            exerciseArgument = exerciseArgument,
            exerciseArgumentCompression = exerciseArgumentCompression,
            exerciseResult = exerciseResult,
            exerciseResultCompression = exerciseResultCompression,
            exerciseActors = ArraySeq.unsafeWrapArray(
              exerciseActors.map(stringInterning.party.unsafe.externalize)
            ),
            exerciseChildEventIds = ArraySeq.unsafeWrapArray(exerciseChildEventIds),
            eventWitnesses = ArraySeq.unsafeWrapArray(
              eventWitnesses.view
                .filter(allQueryingParties)
                .map(stringInterning.party.unsafe.externalize)
                .toArray
            ),
          ),
        )
    }

  def rawTreeEventParser(
      allQueryingParties: Set[Int],
      stringInterning: StringInterning,
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent]] =
    createdTreeEventParser(allQueryingParties, stringInterning) | exercisedTreeEventParser(
      allQueryingParties,
      stringInterning,
    )

  val selectColumnsForTransactionTreeCreate: String = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "event_sequential_id",
    "event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "workflow_id",
    "create_argument",
    "create_argument_compression",
    "create_signatories",
    "create_observers",
    "create_agreement_text",
    "create_key_value",
    "create_key_hash",
    "create_key_value_compression",
    "NULL as exercise_choice",
    "NULL as exercise_argument",
    "NULL as exercise_argument_compression",
    "NULL as exercise_result",
    "NULL as exercise_result_compression",
    "NULL as exercise_actors",
    "NULL as exercise_child_event_ids",
    "submitters",
  ).mkString(", ")

  val selectColumnsForTransactionTreeExercise: String = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "event_sequential_id",
    "event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "workflow_id",
    "NULL as create_argument",
    "NULL as create_argument_compression",
    "NULL as create_signatories",
    "NULL as create_observers",
    "NULL as create_agreement_text",
    "create_key_value",
    "NULL as create_key_hash",
    "create_key_value_compression",
    "exercise_choice",
    "exercise_argument",
    "exercise_argument_compression",
    "exercise_result",
    "exercise_result_compression",
    "exercise_actors",
    "exercise_child_event_ids",
    "submitters",
  ).mkString(", ")

  val EventSequentailIdFromTo: RowParser[(Long, Long)] =
    long("event_sequential_id_from") ~ long("event_sequential_id_to") map {
      case event_sequential_id_from ~ event_sequential_id_to =>
        (event_sequential_id_from, event_sequential_id_to)
    }

}

abstract class EventStorageBackendTemplate(
    eventStrategy: EventStrategy,
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    // This method is needed in pruneEvents, but belongs to [[ParameterStorageBackend]].
    participantAllDivulgedContractsPrunedUpToInclusive: Connection => Option[Offset],
) extends EventStorageBackend {
  import com.daml.platform.store.backend.Conversions.ArrayColumnToIntArray._

  import EventStorageBackendTemplate._

  override def transactionPointwiseQueries: TransactionPointwiseQueries =
    new TransactionPointwiseQueries(
      queryStrategy = queryStrategy,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
    )

  override def transactionStreamingQueries: TransactionStreamingQueries =
    new TransactionStreamingQueries(
      queryStrategy = queryStrategy,
      stringInterning = stringInterning,
    )

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  override def activeContractEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Ref.Party],
      endInclusive: Long,
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    val allInternedFilterParties = allFilterParties.iterator
      .map(stringInterning.party.tryInternalize)
      .flatMap(_.iterator)
      .toSet
    SQL"""
      SELECT
        #$selectColumnsForACSEvents,
        flat_event_witnesses as event_witnesses,
        '' AS command_id
      FROM
        participant_events_create create_evs
      WHERE
        create_evs.event_sequential_id ${queryStrategy.anyOf(eventSequentialIds)}
        AND NOT EXISTS (  -- check not archived as of snapshot
          SELECT 1
          FROM participant_events_consuming_exercise consuming_evs
          WHERE
            create_evs.contract_id = consuming_evs.contract_id
            AND consuming_evs.event_sequential_id <= $endInclusive
        )
      ORDER BY
        create_evs.event_sequential_id -- deliver in index order
      """
      .asVectorOf(rawFlatEventParser(allInternedFilterParties, stringInterning))(connection)
  }

  // TODO pbatko: Consider renaming filter tables from '...filter...' tables to  '...id_filter_...'
  // TODO etq: Implement pruning queries in terms of event sequential id in order to be able to drop offset based indices.
  /** Deletes a subset of the indexed data (up to the pruning offset) in the following order and in the manner specified:
    * 1.a if pruning-all-divulged-contracts is enabled: all divulgence events (retroactive divulgence),
    * 1.b otherwise: divulgence events for which there are archive events (retroactive divulgence),
    * 2. entries from filter for create stakeholders for there is an archive for the corresponding create event,
    * 3. entries from filter for create non-stakeholder informees for there is an archive for the corresponding create event,
    * 4. all entries from filter for consuming stakeholders,
    * 5. all entries from filter for consuming non-stakeholders informees,
    * 6. all entries from filter for non-consuming informees,
    * 7. create events table for which there is an archive event,
    * 8. if pruning-all-divulged-contracts is enabled: create contracts which did not have a locally hosted party before their creation offset (immediate divulgence),
    * 9. all consuming events,
    * 10. all non-consuming events,
    * 11. transaction meta entries for which there exists at least one create event.
    */
  override def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(connection: Connection, loggingContext: LoggingContext): Unit = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement

    if (pruneAllDivulgedContracts) {
      pruneWithLogging(queryDescription = "All retroactive divulgence events pruning") {
        // Note: do not use `QueryStrategy.offsetIsSmallerOrEqual` because divulgence events have a nullable offset
        SQL"""
          -- Retroactive divulgence events
          delete from participant_events_divulgence delete_events
          where delete_events.event_offset <= $pruneUpToInclusive
            or delete_events.event_offset is null
          """
      }(connection, loggingContext)
    } else {
      pruneWithLogging(queryDescription = "Archived retroactive divulgence events pruning") {
        // Note: do not use `QueryStrategy.offsetIsSmallerOrEqual` because divulgence events have a nullable offset
        SQL"""
          -- Retroactive divulgence events (only for contracts archived before the specified offset)
          delete from participant_events_divulgence delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive
            and exists (
              select 1 from participant_events_consuming_exercise archive_events
              where
                archive_events.event_offset <= $pruneUpToInclusive and
                archive_events.contract_id = delete_events.contract_id
            )"""
      }(connection, loggingContext)
    }

    pruneIdFilterTables(pruneUpToInclusive)(connection, loggingContext)

    pruneWithLogging(queryDescription = "Create events pruning") {
      SQL"""
          -- Create events (only for contracts archived before the specified offset)
          delete from participant_events_create delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive AND
                archive_events.contract_id = delete_events.contract_id
            )"""
    }(connection, loggingContext)

    if (pruneAllDivulgedContracts) {
      val pruneAfterClause = {
        // We need to distinguish between the two cases since lexicographical comparison
        // in Oracle doesn't work with '' (empty strings are treated as NULLs) as one of the operands
        participantAllDivulgedContractsPrunedUpToInclusive(connection) match {
          case Some(pruneAfter) => cSQL"and event_offset > $pruneAfter"
          case None => cSQL""
        }
      }

      pruneWithLogging(queryDescription = "Immediate divulgence events pruning") {
        SQL"""
            -- Immediate divulgence pruning
            delete from participant_events_create c
            where event_offset <= $pruneUpToInclusive
            -- Only prune create events which did not have a locally hosted party before their creation offset
            and not exists (
              select 1
              from party_entries p
              where p.typ = 'accept'
              and p.ledger_offset <= c.event_offset
              and #${queryStrategy.isTrue("p.is_local")}
              and #${queryStrategy.arrayContains("c.flat_event_witnesses", "p.party_id")}
            )
            $pruneAfterClause
         """
      }(connection, loggingContext)
    }

    pruneWithLogging(queryDescription = "Exercise (consuming) events pruning") {
      SQL"""
          -- Exercise events (consuming)
          delete from participant_events_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }(connection, loggingContext)

    pruneWithLogging(queryDescription = "Exercise (non-consuming) events pruning") {
      SQL"""
          -- Exercise events (non-consuming)
          delete from participant_events_non_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }(connection, loggingContext)

    // NOTE: This must be done after pruning create events
    // TODO pbatko: Why?
    pruneWithLogging(queryDescription = "transaction meta pruning") {
      eventStrategy.pruneTransactionMeta(pruneUpToInclusive = pruneUpToInclusive)
    }(connection, loggingContext)
  }

  private def pruneIdFilterTables(pruneUpToInclusive: Offset)(
      connection: Connection,
      loggingContext: LoggingContext,
  ): Unit = {
    pruneWithLogging(queryDescription = "Pruning id filter table for create stakeholders") {
      eventStrategy.pruneIdFilterCreateStakeholders(pruneUpToInclusive)
    }(connection, loggingContext)
    pruneWithLogging(queryDescription =
      "Pruning id filter table for create non-stakeholder informees"
    ) {
      eventStrategy.pruneIdFilterCreateNonStakeholderInformees(pruneUpToInclusive)
    }(connection, loggingContext)
    pruneWithLogging(queryDescription = "Pruning filter table for consuming stakeholders") {
      eventStrategy.pruneIdFilterConsumingStakeholders(pruneUpToInclusive)
    }(connection, loggingContext)
    pruneWithLogging(queryDescription =
      "Pruning filter table for consuming non-stakeholders informees"
    ) {
      eventStrategy.pruneIdFilterConsumingNonStakeholderInformees(pruneUpToInclusive)
    }(connection, loggingContext)
    pruneWithLogging(queryDescription = "Pruning filter table for non-consuming informees") {
      eventStrategy.pruneIdFilterNonConsumingInformees(pruneUpToInclusive)
    }(connection, loggingContext)
  }

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(
      connection: Connection,
      loggingContext: LoggingContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")(loggingContext)
  }

  private val rawTransactionEventParser: RowParser[RawTransactionEvent] = {
    import com.daml.platform.store.backend.Conversions.ArrayColumnToStringArray.arrayColumnToStringArray
    (int("event_kind") ~
      str("transaction_id") ~
      int("node_index") ~
      str("command_id").? ~
      str("workflow_id").? ~
      eventId("event_id") ~
      contractId("contract_id") ~
      int("template_id").? ~
      timestampFromMicros("ledger_effective_time").? ~
      array[Int]("create_signatories").? ~
      array[Int]("create_observers").? ~
      str("create_agreement_text").? ~
      byteArray("create_key_value").? ~
      hashFromHexString("create_key_hash").? ~
      int("create_key_value_compression").? ~
      byteArray("create_argument").? ~
      int("create_argument_compression").? ~
      array[Int]("tree_event_witnesses") ~
      array[Int]("flat_event_witnesses") ~
      array[Int]("submitters").? ~
      str("exercise_choice").? ~
      byteArray("exercise_argument").? ~
      int("exercise_argument_compression").? ~
      byteArray("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[Int]("exercise_actors").? ~
      array[String]("exercise_child_event_ids").? ~
      long("event_sequential_id") ~
      offset("event_offset")).map {
      case eventKind ~ transactionId ~ nodeIndex ~ commandId ~ workflowId ~ eventId ~ contractId ~ templateId ~ ledgerEffectiveTime ~ createSignatories ~
          createObservers ~ createAgreementText ~ createKeyValue ~ createKeyHash ~ createKeyCompression ~
          createArgument ~ createArgumentCompression ~ treeEventWitnesses ~ flatEventWitnesses ~ submitters ~ qualifiedChoiceName ~
          exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~
          exerciseChildEventIds ~ eventSequentialId ~ offset =>
        RawTransactionEvent(
          eventKind,
          transactionId,
          nodeIndex,
          commandId,
          workflowId,
          eventId,
          contractId,
          templateId.map(stringInterning.templateId.externalize),
          ledgerEffectiveTime,
          createSignatories.map(_.map(stringInterning.party.unsafe.externalize)),
          createObservers.map(_.map(stringInterning.party.unsafe.externalize)),
          createAgreementText,
          createKeyValue,
          createKeyHash,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          treeEventWitnesses.view.map(stringInterning.party.unsafe.externalize).toSet,
          flatEventWitnesses.view.map(stringInterning.party.unsafe.externalize).toSet,
          submitters
            .map(_.view.map(stringInterning.party.unsafe.externalize).toSet)
            .getOrElse(Set.empty),
          qualifiedChoiceName,
          exerciseArgument,
          exerciseArgumentCompression,
          exerciseResult,
          exerciseResultCompression,
          exerciseActors.map(_.map(stringInterning.party.unsafe.externalize)),
          exerciseChildEventIds,
          eventSequentialId,
          offset,
        )
    }
  }

  override def rawEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[RawTransactionEvent] = {
    SQL"""
       (SELECT
           10 as event_kind,
           transaction_id,
           node_index,
           command_id,
           workflow_id,
           event_id,
           contract_id,
           template_id,
           ledger_effective_time,
           create_signatories,
           create_observers,
           create_agreement_text,
           create_key_value,
           create_key_hash,
           create_key_value_compression,
           create_argument,
           create_argument_compression,
           tree_event_witnesses,
           flat_event_witnesses,
           submitters,
           NULL as exercise_choice,
           NULL as exercise_argument,
           NULL as exercise_argument_compression,
           NULL as exercise_result,
           NULL as exercise_result_compression,
           NULL as exercise_actors,
           NULL as exercise_child_event_ids,
           event_sequential_id,
           event_offset
       FROM
           participant_events_create
       WHERE
           event_sequential_id > $startExclusive
           and event_sequential_id <= $endInclusive)
       UNION ALL
       (SELECT
           20 as event_kind,
           transaction_id,
           node_index,
           command_id,
           workflow_id,
           event_id,
           contract_id,
           template_id,
           ledger_effective_time,
           NULL as create_signatories,
           NULL as create_observers,
           NULL as create_agreement_text,
           create_key_value,
           NULL as create_key_hash,
           create_key_value_compression,
           NULL as create_argument,
           NULL as create_argument_compression,
           tree_event_witnesses,
           flat_event_witnesses,
           submitters,
           exercise_choice,
           exercise_argument,
           exercise_argument_compression,
           exercise_result,
           exercise_result_compression,
           exercise_actors,
           exercise_child_event_ids,
           event_sequential_id,
           event_offset
       FROM
           participant_events_consuming_exercise
       WHERE
           event_sequential_id > $startExclusive
           and event_sequential_id <= $endInclusive)
       UNION ALL
       (SELECT
           25 as event_kind,
           transaction_id,
           node_index,
           command_id,
           workflow_id,
           event_id,
           contract_id,
           template_id,
           ledger_effective_time,
           NULL as create_signatories,
           NULL as create_observers,
           NULL as create_agreement_text,
           create_key_value,
           NULL as create_key_hash,
           create_key_value_compression,
           NULL as create_argument,
           NULL as create_argument_compression,
           tree_event_witnesses,
           flat_event_witnesses,
           submitters,
           exercise_choice,
           exercise_argument,
           exercise_argument_compression,
           exercise_result,
           exercise_result_compression,
           exercise_actors,
           exercise_child_event_ids,
           event_sequential_id,
           event_offset
       FROM
           participant_events_non_consuming_exercise
       WHERE
           event_sequential_id > $startExclusive
           and event_sequential_id <= $endInclusive)
       ORDER BY event_sequential_id ASC"""
      .asVectorOf(rawTransactionEventParser)(connection)
  }

  override def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    SQL"""
         SELECT
            event_sequential_id_to
         FROM
            participant_transaction_meta
         WHERE
            event_offset = (SELECT MAX(event_offset) FROM participant_transaction_meta WHERE event_offset <= $offset)
       """.as(get[Long](1).singleOpt)(connection)
  }

}

/** This encapsulates the moving part as composing various Events queries.
  */
trait EventStrategy {

  /** Generates a clause that checks whether any of the given wildcard parties is a witness
    *
    * @param witnessesColumnName name of the Array column holding witnesses
    * @param internedWildcardParties List of all wildcard parties (their interned names).
    *                                Guaranteed to be non-empty.
    * @return the composable SQL
    */
  def wildcardPartiesClause(
      witnessesColumnName: String,
      internedWildcardParties: Set[Int],
  ): CompositeSql

  /** Generates a clause that checks whether the given parties+templates filter matches the contract,
    *  i.e., whether any of the template ids matches AND any of the parties is a witness
    *
    * @param witnessesColumnName Name of the Array column holding witnesses
    * @param internedParties The non-empty list of interned party names
    * @param internedTemplates The non-empty list of interned template names
    * @return the composable SQL for this filter
    */
  def partiesAndTemplatesClause(
      witnessesColumnName: String,
      internedParties: Set[Int],
      internedTemplates: Set[Int],
  ): CompositeSql

  /** Pruning participant_events_create_filter entries.
    *
    * @param pruneUpToInclusive create and archive events must be earlier or equal to this offset
    * @return the executable anorm query
    */
  def pruneIdFilterCreateStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row]
  def pruneIdFilterCreateNonStakeholderInformees(pruneUpToInclusive: Offset): SimpleSql[Row]

  def pruneIdFilterConsumingStakeholders(pruneUpToInclusive: Offset): SimpleSql[Row]
  def pruneIdFilterConsumingNonStakeholderInformees(pruneUpToInclusive: Offset): SimpleSql[Row]

  def pruneIdFilterNonConsumingInformees(pruneUpToInclusive: Offset): SimpleSql[Row]

  /** Callers can call it only once pruning of create, consuming and non-consuming event tables has already finished.
    * Implementors can assume that these tables have already been pruned.
    */
  def pruneTransactionMeta(pruneUpToInclusive: Offset): SimpleSql[Row]
}
