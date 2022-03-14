// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update
import com.daml.ledger.participant.state.v2.Update.TransactionAccepted
import com.daml.lf.engine.Blinding
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.Transaction.ChildrenRecursion
import com.daml.logging.ContextualizedLogger
import com.daml.platform.store.appendonlydao.events._
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker

object Stream {
  private val logger = ContextualizedLogger.get(getClass)
  def flow(
      initSeqId: Long
  ): Flow[(Offset, Update), ((Offset, Long), TransactionLogUpdate), NotUsed] =
    Flow[(Offset, Update)]
      .map(TransactionUpdateToTransactionLogUpdate.apply)
      .scan(
        TransactionLogUpdate.LedgerEndMarker(Offset.beforeBegin, initSeqId): TransactionLogUpdate
      )((prev, current) => {
        var currSeqId = prev match {
          case TransactionLogUpdate.Transaction(_, _, _, _, events) =>
            events.last.eventSequentialId
          case TransactionLogUpdate.LedgerEndMarker(_, lastEventSeqId) => lastEventSeqId
        }
        current match {
          case transaction: TransactionLogUpdate.Transaction =>
            transaction.copy(events = transaction.events.flatMap {
              case _: TransactionLogUpdate.DivulgenceEvent =>
                currSeqId += 1
                // don't forward divulgence events
                Iterator.empty
              case create: TransactionLogUpdate.CreatedEvent =>
                currSeqId += 1
                Iterator(create.copy(eventSequentialId = currSeqId))
              case exercise: TransactionLogUpdate.ExercisedEvent =>
                currSeqId += 1
                Iterator(exercise.copy(eventSequentialId = currSeqId))
              case unchanged => Iterator(unchanged)
            })
          case ledgerEndMarker: LedgerEndMarker =>
            ledgerEndMarker.copy(lastEventSeqId = currSeqId)
        }
      })
      .drop(1)
      .flatMapConcat {
        case transaction: TransactionLogUpdate.Transaction =>
          Source.fromIterator(() => {
            val eventSequentialId = transaction.events.last.eventSequentialId
            val offset = transaction.offset
            val ledgerEndMarker = TransactionLogUpdate.LedgerEndMarker(offset, eventSequentialId)
            if (transaction.events.nonEmpty) // Divulgence-only transactions should not be forwarded
              Iterator(
                (offset -> eventSequentialId, transaction),
                (offset -> eventSequentialId, ledgerEndMarker),
              )
            else Iterator((offset -> eventSequentialId, ledgerEndMarker))
          })
        case ledgerEndMarker: TransactionLogUpdate.LedgerEndMarker =>
          Source.fromIterator(() =>
            Iterator((ledgerEndMarker.offset -> ledgerEndMarker.lastEventSeqId, ledgerEndMarker))
          )
      }
      .map { case up @ ((offset, seqId), update) =>
        logger.withoutContext.info(
          s"Forwarding log update at $offset, $seqId for ${update.getClass}"
        )
        up
      }
}

object TransactionUpdateToTransactionLogUpdate {
  def apply: UpdateToTransactionLogUpdate = {
    case (
          offset,
          u @ TransactionAccepted(
            _,
            transactionMeta,
            transaction,
            transactionId,
            _,
            _,
            _,
          ),
        ) =>
      val rawEvents = transaction.transaction
        .foldInExecutionOrder(List.empty[(NodeId, Node)])(
          exerciseBegin = (acc, nid, node) => ((nid -> node) :: acc, ChildrenRecursion.DoRecurse),
          // Rollback nodes are not included in the indexer
          rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
          leaf = (acc, nid, node) => (nid -> node) :: acc,
          exerciseEnd = (acc, _, _) => acc,
          rollbackEnd = (acc, _, _) => acc,
        )
        .reverse
        .iterator

      val blinding = u.blindingInfo.getOrElse(Blinding.blind(u.transaction))

      val logUpdates = rawEvents.collect {
        case (nodeId, create: Create) =>
          TransactionLogUpdate.CreatedEvent(
            eventOffset = offset,
            transactionId = transactionId,
            nodeIndex = nodeId.index,
            eventSequentialId = 0L,
            eventId = EventId(u.transactionId, nodeId),
            contractId = create.coid,
            ledgerEffectiveTime = u.transactionMeta.ledgerEffectiveTime,
            templateId = create.templateId,
            commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
            workflowId = u.transactionMeta.workflowId.map(_.toString).getOrElse(""),
            contractKey =
              create.key.map(k => com.daml.lf.transaction.Versioned(create.version, k.key)),
            treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString),
            flatEventWitnesses = create.stakeholders.map(_.toString),
            submitters = u.optCompletionInfo
              .map(_.actAs.iterator.map(_.toString).toSet)
              .getOrElse(Set.empty),
            createArgument = com.daml.lf.transaction.Versioned(create.version, create.arg),
            createSignatories = create.signatories.map(_.toString),
            createObservers = create.stakeholders.diff(create.signatories).map(_.toString),
            createAgreementText = Some(create.agreementText).filter(_.nonEmpty),
          )
        case (nodeId, exercise: Exercise) =>
          TransactionLogUpdate.ExercisedEvent(
            eventOffset = offset,
            transactionId = transactionId,
            nodeIndex = nodeId.index,
            eventSequentialId = 0L,
            eventId = EventId(u.transactionId, nodeId),
            contractId = exercise.targetCoid,
            ledgerEffectiveTime = u.transactionMeta.ledgerEffectiveTime,
            templateId = exercise.templateId,
            commandId = u.optCompletionInfo.map(_.commandId).getOrElse(""),
            workflowId = u.transactionMeta.workflowId.map(_.toString).getOrElse(""),
            contractKey =
              exercise.key.map(k => com.daml.lf.transaction.Versioned(exercise.version, k.key)),
            treeEventWitnesses = blinding.disclosure.getOrElse(nodeId, Set.empty).map(_.toString),
            flatEventWitnesses =
              if (exercise.consuming) exercise.stakeholders.map(_.toString) else Set.empty,
            submitters = u.optCompletionInfo
              .map(_.actAs.iterator.map(_.toString).toSet)
              .getOrElse(Set.empty),
            choice = exercise.choiceId,
            actingParties = exercise.actingParties,
            children = exercise.children.iterator
              .map(EventId(u.transactionId, _).toLedgerString.toString)
              .toSeq,
            exerciseArgument = exercise.versionedChosenValue,
            exerciseResult = exercise.versionedExerciseResult,
            consuming = exercise.consuming,
          )
      }

      val events = logUpdates ++ blinding.divulgence.iterator.collect {
        // only store divulgence events, which are divulging to parties
        case (_, visibleToParties) if visibleToParties.nonEmpty =>
          TransactionLogUpdate.DivulgenceEvent(
            eventOffset = offset,
            eventSequentialId = 0L,
            transactionId = null,
            eventId = null,
            commandId = null,
            workflowId = null,
            ledgerEffectiveTime = null,
            treeEventWitnesses = null,
            flatEventWitnesses = null,
            submitters = null,
            templateId = null,
            contractId = null,
          )
      }

      TransactionLogUpdate.Transaction(
        transactionId = transactionId,
        workflowId = transactionMeta.workflowId.getOrElse(""), // TODO check
        effectiveAt = transactionMeta.ledgerEffectiveTime,
        offset = offset,
        events = events.toVector,
      )

    case (offset, _) => LedgerEndMarker(offset, 0L)
  }

  type UpdateToTransactionLogUpdate =
    ((Offset, Update)) => TransactionLogUpdate
}
