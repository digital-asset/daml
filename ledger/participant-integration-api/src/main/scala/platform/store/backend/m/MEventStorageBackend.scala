// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.ledger.offset.Offset
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.data.Ref.{Party, TransactionId}
import com.daml.lf.ledger.EventId
import com.daml.logging.LoggingContext
import com.daml.platform.store.appendonlydao.events.{ContractId, EventsTable, Raw}
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.{DbDto, EventStorageBackend}
import com.daml.platform.store.cache.LedgerEndCache

import scala.collection.Searching.{Found, InsertionPoint}
import scala.collection.compat.immutable.ArraySeq

import DbDtoUtils._

class MEventStorageBackend(ledgerEndCache: LedgerEndCache) extends EventStorageBackend {

  override def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(connection: Connection, loggingContext: LoggingContext): Unit = {
    val store = MStore(connection)
    val pruneUpToInclusiveString = pruneUpToInclusive.toHexString.toString
    store.synchronized {
      val base = store.mData
      store.mData = base.copy(
        contractIdIndex = base.contractIdIndex.iterator.flatMap { case (contractId, dtos) =>
          val archived = dtos.exists {
            case dto: DbDto.EventExercise =>
              dto.consuming && dto.event_offset.exists(_ <= pruneUpToInclusiveString)
            case _ => false
          }
          dtos.filterNot {
            case dto: DbDto.EventDivulgence =>
              (pruneAllDivulgedContracts && dto.event_offset.exists(
                _ <= pruneUpToInclusiveString
              )) ||
                (!pruneAllDivulgedContracts && archived)
            case dto: DbDto.EventCreate =>
              archived ||
                (pruneAllDivulgedContracts &&
                  dto.event_offset.exists(_ <= pruneUpToInclusiveString) &&
                  !dto.flat_event_witnesses.exists(witness =>
                    base.partyIndex
                      .get(witness)
                      .exists(
                        _.filter(_.ledger_offset <= dto.event_offset.get)
                          .exists(_.is_local.contains(true))
                      )
                  ))
            case dto: DbDto.EventExercise => dto.event_offset.exists(_ <= pruneUpToInclusiveString)
            case _ => throw new Exception
          } match {
            case empty if empty.isEmpty => Iterator.empty
            case nonEmpty => Iterator(contractId -> nonEmpty)
          }
        }.toMap
      )
    }
  }

  override def isPruningOffsetValidAgainstMigration(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      connection: Connection,
  ): Boolean =
    true

  override def transactionEvents(
      rangeParams: EventStorageBackend.RangeParams,
      filterParams: EventStorageBackend.FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    transactions(rangeParams, filterParams, forFlat, toRawFlatEvent)(connection)

  override def transactionTreeEvents(
      rangeParams: EventStorageBackend.RangeParams,
      filterParams: EventStorageBackend.FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] =
    transactions(rangeParams, filterParams, forTree, toRawTreeEvent)(connection)

  private def transactions[T](
      rangeParams: EventStorageBackend.RangeParams,
      filterParams: EventStorageBackend.FilterParams,
      visibility: FilterParams => (DbDto => Boolean),
      toEvent: Set[String] => DbDto => EventsTable.Entry[T],
  )(connection: Connection): Vector[EventsTable.Entry[T]] = {
    val mStore = MStore(connection)
    val mData = mStore.mData
    val forVisibility = visibility(filterParams)
    val allQueryingParties =
      filterParams.wildCardParties.iterator
        .++(filterParams.partiesAndTemplates.iterator.flatMap(_._1.iterator))
        .map(_.toString)
        .toSet
    mData
      .eventRange(rangeParams.startExclusive, rangeParams.endInclusive)
      .filter(forVisibility)
      .take(rangeParams.limit.getOrElse(Integer.MAX_VALUE))
      .map(toEvent(allQueryingParties))
      .toVector
  }

  override def activeContractEvents(
      rangeParams: EventStorageBackend.RangeParams,
      filterParams: EventStorageBackend.FilterParams,
      endInclusiveOffset: Offset,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    throw new UnsupportedOperationException

  override def activeContractEventIds(
      partyFilter: Party,
      templateIdFilter: Option[Ref.Identifier],
      startExclusive: Long,
      endInclusive: Long,
      limit: Int,
  )(connection: Connection): Vector[Long] = {
    val mStore = MStore(connection)
    val mData = mStore.mData
    val forVisibility = forFlat(templateIdFilter match {
      case Some(templateId) => FilterParams(Set.empty, Set(Set(partyFilter) -> Set(templateId)))
      case None => FilterParams(Set(partyFilter), Set.empty)
    })
    mData
      .eventRange(startExclusive, endInclusive)
      .filter(forVisibility)
      .collect { case dbDto: DbDto.EventCreate => dbDto.event_sequential_id }
      .take(limit)
      .toVector
  }

  override def activeContractEventBatch(
      eventSequentialIds: Iterable[Long],
      allFilterParties: Set[Party],
      endInclusive: Long,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    val mStore = MStore(connection)
    val mData = mStore.mData
    val allQueryingParties = allFilterParties.map(_.toString)
    eventSequentialIds.iterator
      .map(i => mData.events(i.toInt - 1))
      .filterNot(mData.archived(endInclusive))
      .map(toRawFlatEvent(allQueryingParties))
      .toVector
  }

  override def flatTransaction(
      transactionId: TransactionId,
      filterParams: EventStorageBackend.FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] =
    transaction(transactionId, filterParams, forFlat, toRawFlatEvent)(connection)

  override def transactionTree(
      transactionId: TransactionId,
      filterParams: EventStorageBackend.FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] =
    transaction(transactionId, filterParams, forTree, toRawTreeEvent)(connection)

  private def transaction[T](
      transactionId: TransactionId,
      filterParams: EventStorageBackend.FilterParams,
      visibility: FilterParams => (DbDto => Boolean),
      toEvent: Set[String] => DbDto => EventsTable.Entry[T],
  )(connection: Connection): Vector[EventsTable.Entry[T]] = {
    val mStore = MStore(connection)
    val mData = mStore.mData
    val ledgerEnd = ledgerEndCache()
    val prunedUptoInclusiveOffset = mStore.prunedUpToInclusive
    val forVisibility = visibility(filterParams)
    val allQueryingParties =
      filterParams.wildCardParties.iterator
        .++(filterParams.partiesAndTemplates.iterator.flatMap(_._1.iterator))
        .map(_.toString)
        .toSet
    mData.transactionIdIndex
      .getOrElse(transactionId.toString, Vector.empty)
      .iterator
      .filter(forVisibility)
      .takeWhile(dto =>
        prunedUptoInclusiveOffset == null || (dto.offset > prunedUptoInclusiveOffset.toHexString &&
          dto.eventSeqId <= ledgerEnd._2)
      )
      .map(toEvent(allQueryingParties))
      .toVector
  }

  override def maxEventSequentialIdOfAnObservableEvent(
      offset: Offset
  )(connection: Connection): Option[Long] = {
    val stringOffset = offset.toHexString.toString
    val events = MStore(connection).mData.events
    if (stringOffset == "") {
      if (events.isEmpty) None
      else Some(0L)
    } else {
      (SearchUtil.rightMostBinarySearch[DbDto, String](
        stringOffset,
        events,
        _.offset,
      ) match {
        case Found(i) => Some(events(i))
        case InsertionPoint(i) if i == 0 => None
        case InsertionPoint(i) => Some(events(i - 1))
      }).map(_.eventSeqId)
    }
  }

  override def rawEvents(
      startExclusive: Long,
      endInclusive: Long,
  )(connection: Connection): Vector[EventStorageBackend.RawTransactionEvent] = {
    val mStore = MStore(connection)
    val mData = mStore.mData
    mData
      .eventRange(startExclusive, endInclusive)
      .collect {
        case dto: DbDto.EventCreate =>
          EventStorageBackend.RawTransactionEvent(
            eventKind = 10,
            transactionId = dto.transaction_id.get,
            nodeIndex = dto.node_index.get,
            commandId = dto.command_id,
            workflowId = dto.workflow_id,
            eventId = EventId.assertFromString(dto.event_id.get),
            contractId = ContractId.assertFromString(dto.contract_id),
            templateId = dto.template_id.map(Ref.Identifier.assertFromString),
            ledgerEffectiveTime = dto.ledger_effective_time.map(Time.Timestamp(_)),
            createSignatories = dto.create_signatories.map(_.toArray),
            createObservers = dto.create_observers.map(_.toArray),
            createAgreementText = dto.create_agreement_text,
            createKeyValue = dto.create_key_value,
            createKeyCompression = dto.create_key_value_compression,
            createArgument = dto.create_argument,
            createArgumentCompression = dto.create_argument_compression,
            treeEventWitnesses = dto.tree_event_witnesses,
            flatEventWitnesses = dto.flat_event_witnesses,
            submitters = dto.submitters.getOrElse(Set.empty),
            exerciseChoice = None,
            exerciseArgument = None,
            exerciseArgumentCompression = None,
            exerciseResult = None,
            exerciseResultCompression = None,
            exerciseActors = None,
            exerciseChildEventIds = None,
            eventSequentialId = dto.event_sequential_id,
            offset = Offset.fromHexString(Ref.HexString.assertFromString(dto.event_offset.get)),
          )

        case dto: DbDto.EventExercise =>
          EventStorageBackend.RawTransactionEvent(
            eventKind = if (dto.consuming) 20 else 25,
            transactionId = dto.transaction_id.get,
            nodeIndex = dto.node_index.get,
            commandId = dto.command_id,
            workflowId = dto.workflow_id,
            eventId = EventId.assertFromString(dto.event_id.get),
            contractId = ContractId.assertFromString(dto.contract_id),
            templateId = dto.template_id.map(Ref.Identifier.assertFromString),
            ledgerEffectiveTime = dto.ledger_effective_time.map(Time.Timestamp(_)),
            createSignatories = None,
            createObservers = None,
            createAgreementText = None,
            createKeyValue = dto.create_key_value,
            createKeyCompression = dto.create_key_value_compression,
            createArgument = None,
            createArgumentCompression = None,
            treeEventWitnesses = dto.tree_event_witnesses,
            flatEventWitnesses = dto.flat_event_witnesses,
            submitters = dto.submitters.getOrElse(Set.empty),
            exerciseChoice = dto.exercise_choice,
            exerciseArgument = dto.exercise_argument,
            exerciseArgumentCompression = dto.exercise_argument_compression,
            exerciseResult = dto.exercise_result,
            exerciseResultCompression = dto.exercise_result_compression,
            exerciseActors = dto.exercise_actors.map(_.toArray),
            exerciseChildEventIds = dto.exercise_child_event_ids.map(_.toArray),
            eventSequentialId = dto.event_sequential_id,
            offset = Offset.fromHexString(Ref.HexString.assertFromString(dto.event_offset.get)),
          )
        case _ => throw new Exception
      }
      .toVector
  }

  private def forFlat(filterParams: FilterParams): DbDto => Boolean =
    forFilterParams(
      filterParams,
      _.flatEventWitnesses,
    )
  private def forTree(filterParams: FilterParams): DbDto => Boolean =
    forFilterParams(
      filterParams,
      _.treeEventWitnesses,
    )
  private def forFilterParams(
      filterParams: FilterParams,
      visibilityExtractor: DbDto => Set[String],
  ): DbDto => Boolean = {
    val stringWildCardParties = filterParams.wildCardParties.map(_.toString)
    val stringPartiesAndTemplates = filterParams.partiesAndTemplates.map {
      case (parties, templates) => (parties.map(_.toString), templates.map(_.toString))
    }
    dto =>
      (dto.isInstanceOf[DbDto.EventCreate] || dto.isInstanceOf[DbDto.EventExercise]) && {
        val visibility = visibilityExtractor(dto)
        visibility.exists(stringWildCardParties) || stringPartiesAndTemplates.exists {
          case (parties, templates) => templates(dto.templateId) && visibility.exists(parties)
        }
      }
  }

  private def toRawFlatEvent(
      allQueryingParties: Set[String]
  )(dbDto: DbDto): EventsTable.Entry[Raw.FlatEvent] = dbDto match {
    case dto: DbDto.EventCreate =>
      EventsTable.Entry(
        eventOffset = Offset.fromHexString(Ref.HexString.assertFromString(dto.event_offset.get)),
        transactionId = dto.transaction_id.get,
        nodeIndex = dto.node_index.get,
        eventSequentialId = dto.event_sequential_id,
        ledgerEffectiveTime = Time.Timestamp(dto.ledger_effective_time.get),
        commandId = dto.command_id
          .filter(commandId =>
            commandId != "" && dto.submitters.getOrElse(Set.empty).exists(allQueryingParties)
          )
          .getOrElse(""),
        workflowId = dto.workflow_id.getOrElse(""),
        event = Raw.FlatEvent.Created(
          eventId = dto.event_id.get,
          contractId = dto.contract_id,
          templateId = Ref.Identifier.assertFromString(dto.template_id.get),
          createArgument = dto.create_argument.get,
          createArgumentCompression = dto.create_argument_compression,
          createSignatories = ArraySeq.unsafeWrapArray(dto.create_signatories.get.toArray),
          createObservers = ArraySeq.unsafeWrapArray(dto.create_observers.get.toArray),
          createAgreementText = dto.create_agreement_text,
          createKeyValue = dto.create_key_value,
          createKeyValueCompression = dto.create_key_value_compression,
          eventWitnesses = ArraySeq.unsafeWrapArray(
            dto.flat_event_witnesses.view
              .filter(allQueryingParties)
              .toArray
          ),
        ),
      )

    case dto: DbDto.EventExercise =>
      EventsTable.Entry(
        eventOffset = Offset.fromHexString(Ref.HexString.assertFromString(dto.event_offset.get)),
        transactionId = dto.transaction_id.get,
        nodeIndex = dto.node_index.get,
        eventSequentialId = dto.event_sequential_id,
        ledgerEffectiveTime = Time.Timestamp(dto.ledger_effective_time.get),
        commandId = dto.command_id
          .filter(commandId =>
            commandId != "" && dto.submitters.getOrElse(Set.empty).exists(allQueryingParties)
          )
          .getOrElse(""),
        workflowId = dto.workflow_id.getOrElse(""),
        event = Raw.FlatEvent.Archived(
          eventId = dto.event_id.get,
          contractId = dto.contract_id,
          templateId = Ref.Identifier.assertFromString(dto.template_id.get),
          eventWitnesses = ArraySeq.unsafeWrapArray(
            dto.flat_event_witnesses.view
              .filter(allQueryingParties)
              .toArray
          ),
        ),
      )

    case _ => throw new Exception
  }

  private def toRawTreeEvent(
      allQueryingParties: Set[String]
  )(dbDto: DbDto): EventsTable.Entry[Raw.TreeEvent] = dbDto match {
    case dto: DbDto.EventCreate =>
      EventsTable.Entry(
        eventOffset = Offset.fromHexString(Ref.HexString.assertFromString(dto.event_offset.get)),
        transactionId = dto.transaction_id.get,
        nodeIndex = dto.node_index.get,
        eventSequentialId = dto.event_sequential_id,
        ledgerEffectiveTime = Time.Timestamp(dto.ledger_effective_time.get),
        commandId = dto.command_id
          .filter(commandId =>
            commandId != "" && dto.submitters.getOrElse(Set.empty).exists(allQueryingParties)
          )
          .getOrElse(""),
        workflowId = dto.workflow_id.getOrElse(""),
        event = Raw.TreeEvent.Created(
          eventId = dto.event_id.get,
          contractId = dto.contract_id,
          templateId = Ref.Identifier.assertFromString(dto.template_id.get),
          createArgument = dto.create_argument.get,
          createArgumentCompression = dto.create_argument_compression,
          createSignatories = ArraySeq.unsafeWrapArray(dto.create_signatories.get.toArray),
          createObservers = ArraySeq.unsafeWrapArray(dto.create_observers.get.toArray),
          createAgreementText = dto.create_agreement_text,
          createKeyValue = dto.create_key_value,
          createKeyValueCompression = dto.create_key_value_compression,
          eventWitnesses = ArraySeq.unsafeWrapArray(
            dto.tree_event_witnesses.view
              .filter(allQueryingParties)
              .toArray
          ),
        ),
      )

    case dto: DbDto.EventExercise =>
      EventsTable.Entry(
        eventOffset = Offset.fromHexString(Ref.HexString.assertFromString(dto.event_offset.get)),
        transactionId = dto.transaction_id.get,
        nodeIndex = dto.node_index.get,
        eventSequentialId = dto.event_sequential_id,
        ledgerEffectiveTime = Time.Timestamp(dto.ledger_effective_time.get),
        commandId = dto.command_id
          .filter(commandId =>
            commandId != "" && dto.submitters.getOrElse(Set.empty).exists(allQueryingParties)
          )
          .getOrElse(""),
        workflowId = dto.workflow_id.getOrElse(""),
        event = Raw.TreeEvent.Exercised(
          eventId = dto.event_id.get,
          contractId = dto.contract_id,
          templateId = Ref.Identifier.assertFromString(dto.template_id.get),
          eventWitnesses = ArraySeq.unsafeWrapArray(
            dto.tree_event_witnesses.view
              .filter(allQueryingParties)
              .toArray
          ),
          exerciseConsuming = dto.consuming,
          exerciseChoice = dto.exercise_choice.get,
          exerciseArgument = dto.exercise_argument.get,
          exerciseArgumentCompression = dto.exercise_argument_compression,
          exerciseResultCompression = dto.exercise_result_compression,
          exerciseActors = ArraySeq.unsafeWrapArray(dto.exercise_actors.get.toArray),
          exerciseChildEventIds =
            ArraySeq.unsafeWrapArray(dto.exercise_child_event_ids.get.toArray),
          exerciseResult = dto.exercise_result,
        ),
      )

    case _ => throw new Exception
  }
}
