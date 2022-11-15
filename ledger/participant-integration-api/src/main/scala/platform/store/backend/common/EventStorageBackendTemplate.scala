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
import com.daml.platform.store.ChoiceCoder
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
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import com.daml.platform.store.backend.EventStorageBackend.RawTransactionEvent
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

import scala.annotation.nowarn
import scala.collection.immutable.ArraySeq

abstract class EventStorageBackendTemplate(
    eventStrategy: EventStrategy,
    queryStrategy: QueryStrategy,
    ledgerEndCache: LedgerEndCache,
    stringInterning: StringInterning,
    // This method is needed in pruneEvents, but belongs to [[ParameterStorageBackend]].
    participantAllDivulgedContractsPrunedUpToInclusive: Connection => Option[Offset],
) extends EventStorageBackend {
  import com.daml.platform.store.backend.Conversions.ArrayColumnToIntArray._
  import com.daml.platform.store.backend.Conversions.ArrayColumnToStringArray._

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

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

  private val selectColumnsForFlatTransactionsCreate =
    baseColumnsForFlatTransactionsCreate.mkString(", ")

  private val selectColumnsForFlatTransactionsExercise =
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
      allQueryingParties: Set[Int]
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
      allQueryingParties: Set[Int]
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

  private def rawFlatEventParser(
      allQueryingParties: Set[Int]
  ): RowParser[EventStorageBackend.Entry[Raw.FlatEvent]] =
    createdFlatEventParser(allQueryingParties) | archivedFlatEventParser(allQueryingParties)

  private def createdTreeEventParser(
      allQueryingParties: Set[Int]
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
      allQueryingParties: Set[Int]
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent.Exercised]] =
    exercisedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ submitters ~ exerciseConsuming ~ exerciseChoice ~ exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~ exerciseChildEventIds =>
        val (interfaceId, choiceName) =
          ChoiceCoder.decode(exerciseChoice): @nowarn("msg=deprecated")
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

  private def rawTreeEventParser(
      allQueryingParties: Set[Int]
  ): RowParser[EventStorageBackend.Entry[Raw.TreeEvent]] =
    createdTreeEventParser(allQueryingParties) | exercisedTreeEventParser(allQueryingParties)

  private val selectColumnsForTransactionTreeCreate = Seq(
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

  private val selectColumnsForTransactionTreeExercise = Seq(
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

  private def events[T](
      joinClause: CompositeSql,
      additionalAndClause: CompositeSql,
      rowParser: Set[Int] => RowParser[T],
      witnessesColumn: String,
      partitions: List[(String, String)],
  )(
      limit: Option[Int],
      fetchSizeHint: Option[Int],
      filterParams: FilterParams,
  )(connection: Connection): Vector[T] = {
    val internedAllParties: Set[Int] =
      filterParams.wildCardParties.iterator
        .++(filterParams.partiesAndTemplates.iterator.flatMap(_._1.iterator))
        .map(stringInterning.party.tryInternalize)
        .flatMap(_.iterator)
        .toSet

    val internedWildcardParties: Set[Int] = filterParams.wildCardParties.view
      .flatMap(party => stringInterning.party.tryInternalize(party).toList)
      .toSet

    val internedPartiesAndTemplates: List[(Set[Int], Set[Int])] =
      filterParams.partiesAndTemplates.iterator
        .map { case (parties, templateIds) =>
          (
            parties.flatMap(s => stringInterning.party.tryInternalize(s).toList),
            templateIds.flatMap(s => stringInterning.templateId.tryInternalize(s).toList),
          )
        }
        .filterNot(_._1.isEmpty)
        .filterNot(_._2.isEmpty)
        .toList

    if (internedWildcardParties.isEmpty && internedPartiesAndTemplates.isEmpty) {
      Vector.empty
    } else {
      val wildcardPartiesClause = if (internedWildcardParties.isEmpty) {
        Nil
      } else {
        eventStrategy.wildcardPartiesClause(witnessesColumn, internedWildcardParties) :: Nil
      }
      val filterPartiesClauses = internedPartiesAndTemplates.map { case (parties, templates) =>
        eventStrategy.partiesAndTemplatesClause(witnessesColumn, parties, templates)
      }

      val witnessesWhereClause =
        (wildcardPartiesClause ::: filterPartiesClauses).mkComposite("(", " or ", ")")

      // NOTE:
      // 1. We use `order by event_sequential_id` to hint Postgres to use an index scan rather than a sequential scan.
      // 2. We also need to wrap this subquery in another subquery because
      // on Oracle subqueries used with `union all` cannot contain an `order by` clause.
      def selectFrom(table: String, selectColumns: String) = cSQL"""
        (SELECT #$selectColumns, event_witnesses, command_id FROM ( SELECT
          #$selectColumns, #$witnessesColumn as event_witnesses, command_id
        FROM
          #$table $joinClause

        WHERE
          $additionalAndClause
          $witnessesWhereClause
         ORDER BY event_sequential_id
         ) x)
      """

      val selectClause = partitions
        .map(p => selectFrom(p._1, p._2))
        .mkComposite("", " UNION ALL", "")

      SQL"""
        $selectClause
        ORDER BY event_sequential_id
        ${QueryStrategy.limitClause(limit)}"""
        .withFetchSize(fetchSizeHint)
        .asVectorOf(rowParser(internedAllParties))(connection)
    }
  }

  override def transactionEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    events(
      joinClause = cSQL"",
      additionalAndClause = cSQL"""
            event_sequential_id > ${rangeParams.startExclusive} AND
            event_sequential_id <= ${rangeParams.endInclusive} AND""",
      rowParser = rawFlatEventParser,
      witnessesColumn = "flat_event_witnesses",
      partitions = List(
        "participant_events_create" -> selectColumnsForFlatTransactionsCreate,
        "participant_events_consuming_exercise" -> selectColumnsForFlatTransactionsExercise,
        // Note: previously we used divulgence events, however they don't have flat event witnesses and were thus never included anyway
        // "participant_events_divulgence" -> selectColumnsForFlatTransactionsDivulgence,
      ),
    )(
      limit = rangeParams.limit,
      fetchSizeHint = rangeParams.fetchSizeHint,
      filterParams,
    )(connection)
  }

  override def activeContractEventIds(
      partyFilter: Ref.Party,
      templateIdFilter: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    (
      stringInterning.party.tryInternalize(partyFilter),
      templateIdFilter.map(stringInterning.templateId.tryInternalize),
    ) match {
      case (None, _) => Vector.empty // partyFilter never seen
      case (_, Some(None)) => Vector.empty // templateIdFilter never seen
      case (Some(internedPartyFilter), internedTemplateIdFilterNested) =>
        val (templateIdFilterClause, templateIdOrderingClause) =
          internedTemplateIdFilterNested.flatten // flatten works for both None, Some(Some(x)) case, Some(None) excluded before
          match {
            case Some(internedTemplateId) =>
              (
                cSQL"AND filters.template_id = $internedTemplateId",
                cSQL"filters.template_id,",
              )
            case None => (cSQL"", cSQL"")
          }
        SQL"""
         SELECT filters.event_sequential_id
         FROM
           participant_events_create_filter filters
         WHERE
           filters.party_id = $internedPartyFilter
           $templateIdFilterClause
           AND $startExclusive < event_sequential_id
           AND event_sequential_id <= $endInclusive
         ORDER BY
           filters.party_id,
           $templateIdOrderingClause
           filters.event_sequential_id -- deliver in index order
         ${QueryStrategy.limitClause(Some(limit))}
       """
          .asVectorOf(long("event_sequential_id"))(connection)
    }
  }

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
      .asVectorOf(rawFlatEventParser(allInternedFilterParties))(connection)
  }

  override def flatTransaction(
      transactionId: Ref.TransactionId,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.backend.Conversions.ledgerStringToStatement
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    val ledgerEndOffset = ledgerEndCache()._1
    events(
      joinClause = cSQL"""JOIN parameters ON
            (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
            AND event_offset <= $ledgerEndOffset""",
      additionalAndClause = cSQL"""
            transaction_id = $transactionId AND""",
      rowParser = rawFlatEventParser,
      witnessesColumn = "flat_event_witnesses",
      partitions = List(
        // we do not want to fetch divulgence events
        "participant_events_create" -> selectColumnsForFlatTransactionsCreate,
        "participant_events_consuming_exercise" -> selectColumnsForFlatTransactionsExercise,
        "participant_events_non_consuming_exercise" -> selectColumnsForFlatTransactionsExercise,
      ),
    )(
      limit = None,
      fetchSizeHint = None,
      filterParams = filterParams,
    )(connection)
  }

  override def transactionTreeEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.TreeEvent]] = {
    events(
      joinClause = cSQL"",
      additionalAndClause = cSQL"""
            event_sequential_id > ${rangeParams.startExclusive} AND
            event_sequential_id <= ${rangeParams.endInclusive} AND""",
      rowParser = rawTreeEventParser,
      witnessesColumn = "tree_event_witnesses",
      partitions = List(
        // we do not want to fetch divulgence events
        "participant_events_create" -> s"$selectColumnsForTransactionTreeCreate, ${queryStrategy
            .constBooleanSelect(false)} as exercise_consuming",
        "participant_events_consuming_exercise" -> s"$selectColumnsForTransactionTreeExercise, ${queryStrategy
            .constBooleanSelect(true)} as exercise_consuming",
        "participant_events_non_consuming_exercise" -> s"$selectColumnsForTransactionTreeExercise, ${queryStrategy
            .constBooleanSelect(false)} as exercise_consuming",
      ),
    )(
      limit = rangeParams.limit,
      fetchSizeHint = rangeParams.fetchSizeHint,
      filterParams,
    )(connection)
  }

  override def transactionTree(
      transactionId: Ref.TransactionId,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventStorageBackend.Entry[Raw.TreeEvent]] = {
    import com.daml.platform.store.backend.Conversions.ledgerStringToStatement
    import com.daml.platform.store.backend.Conversions.OffsetToStatement
    val ledgerEndOffset = ledgerEndCache()._1
    events(
      joinClause = cSQL"""JOIN parameters ON
            (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
            AND event_offset <= $ledgerEndOffset""",
      additionalAndClause = cSQL"""
            transaction_id = $transactionId AND""",
      rowParser = rawTreeEventParser,
      witnessesColumn = "tree_event_witnesses",
      partitions = List(
        // we do not want to fetch divulgence events
        "participant_events_create" -> s"$selectColumnsForTransactionTreeCreate, ${queryStrategy
            .constBooleanSelect(false)} as exercise_consuming",
        "participant_events_consuming_exercise" -> s"$selectColumnsForTransactionTreeExercise, ${queryStrategy
            .constBooleanSelect(true)} as exercise_consuming",
        "participant_events_non_consuming_exercise" -> s"$selectColumnsForTransactionTreeExercise, ${queryStrategy
            .constBooleanSelect(false)} as exercise_consuming",
      ),
    )(
      limit = None,
      fetchSizeHint = None,
      filterParams,
    )(connection)
  }

  // This method is too complex for StorageBackend.
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

    pruneWithLogging(queryDescription = "Create events filter table pruning") {
      eventStrategy.pruneCreateFilters(pruneUpToInclusive)
    }(connection, loggingContext)

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
          createArgument ~ createArgumentCompression ~ treeEventWitnesses ~ flatEventWitnesses ~ submitters ~ exerciseChoice ~
          exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~
          exerciseChildEventIds ~ eventSequentialId ~ offset =>
        val decodedExerciseChoice =
          exerciseChoice.map(ChoiceCoder.decode): @nowarn("msg=deprecated")
        RawTransactionEvent(
          eventKind,
          transactionId,
          nodeIndex,
          commandId,
          workflowId,
          eventId,
          contractId,
          templateId.map(stringInterning.templateId.externalize),
          decodedExerciseChoice.flatMap(_._1),
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
          decodedExerciseChoice.map(_._2),
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
    def selectFrom(table: String) = cSQL"""
      SELECT max(event_sequential_id) AS max_esi FROM #$table
      WHERE event_offset = (select max(event_offset) from #$table where event_offset <= $offset)
    """
    SQL"""SELECT max(max_esi) FROM (
      (${selectFrom("participant_events_consuming_exercise")})
      UNION ALL
      (${selectFrom("participant_events_create")})
      UNION ALL
      (${selectFrom("participant_events_non_consuming_exercise")})
    ) participant_events"""
      .as(get[Long](1).?.single)(connection)
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
  def pruneCreateFilters(pruneUpToInclusive: Offset): SimpleSql[Row]
}
