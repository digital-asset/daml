// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index.internal

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Materializer, RestartSettings}
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext
import com.daml.platform.store.appendonlydao.events.{Contract, ContractStateEvent, Key, Party}
import com.daml.platform.store.interfaces.TransactionLogUpdate

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

private[index] object BuffersUpdater {
  type SubscribeToTransactionLogUpdates =
    Option[(Offset, Long)] => Source[((Offset, Long), TransactionLogUpdate), NotUsed]
  type BuffersIndex = AtomicReference[Option[(Offset, Long)]]

  /** [[BuffersUpdater]] convenience builder.
    *
    * @param subscribeToTransactionLogUpdates Subscribe to the transaction log updates stream starting at a specific
    *                                         `(Offset, EventSequentialId)`.
    * @param updateTransactionsBuffer Trigger externally the update of the transactions buffer.
    * @param updateMutableCache Trigger externally the update of the mutable contract state cache.
    *                           (see [[com.daml.platform.store.cache.MutableCacheBackedContractStore]])
    * @param toContractStateEvents Converts [[TransactionLogUpdate]]s to [[ContractStateEvent]]s.
    * @param minBackoffStreamRestart Minimum back-off before restarting the transaction log updates stream.
    * @param sysExitWithCode Triggers a system exit (i.e. `sys.exit`) with a specific exit code.
    * @param mat The Akka materializer.
    * @param loggingContext The logging context.
    */
  def owner(
      subscribeToTransactionLogUpdates: SubscribeToTransactionLogUpdates,
      updateTransactionsBuffer: (Offset, TransactionLogUpdate) => Unit,
      updateMutableCache: ContractStateEvent => Unit,
      toContractStateEvents: TransactionLogUpdate => Iterator[ContractStateEvent] =
        convertToContractStateEvents,
      minBackoffStreamRestart: FiniteDuration = 100.millis,
      sysExitWithCode: Int => Unit = sys.exit(_),
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[BuffersIndex] = {
    def updateInternal(
        updaterIndex: BuffersIndex
    ): (((Offset, Long), TransactionLogUpdate)) => Unit = {
      case ((offset, eventSequentialId), transactionLogUpdate) =>
        updateTransactionsBuffer(offset, transactionLogUpdate)
        toContractStateEvents(transactionLogUpdate).foreach(updateMutableCache)
        updaterIndex.set(Some(offset -> eventSequentialId))
    }

    for {
      updaterIndex <- ResourceOwner.successful(new BuffersIndex(None))
      _ <- RestartableManagedStream.owner(
        name = "buffers updater",
        streamBuilder = () => subscribeToTransactionLogUpdates(updaterIndex.get),
        restartSettings = RestartSettings(
          minBackoff = minBackoffStreamRestart,
          maxBackoff = 10.seconds,
          randomFactor = 0.0,
        ),
        teardown = sysExitWithCode,
        sink = Sink.foreach(updateInternal(updaterIndex)),
      )
    } yield updaterIndex
  }

  private[index] def convertToContractStateEvents(
      tx: TransactionLogUpdate
  ): Iterator[ContractStateEvent] =
    tx match {
      case tx: TransactionLogUpdate.Transaction =>
        tx.events.iterator.collect {
          case createdEvent: TransactionLogUpdate.CreatedEvent =>
            ContractStateEvent.Created(
              contractId = createdEvent.contractId,
              contract = Contract(
                template = createdEvent.templateId,
                arg = createdEvent.createArgument,
                agreementText = createdEvent.createAgreementText.getOrElse(""),
              ),
              globalKey = createdEvent.contractKey.map(k =>
                Key.assertBuild(createdEvent.templateId, k.unversioned)
              ),
              ledgerEffectiveTime = createdEvent.ledgerEffectiveTime,
              stakeholders = createdEvent.flatEventWitnesses.map(Party.assertFromString),
              eventOffset = createdEvent.eventOffset,
              eventSequentialId = createdEvent.eventSequentialId,
            )
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
            ContractStateEvent.Archived(
              contractId = exercisedEvent.contractId,
              globalKey = exercisedEvent.contractKey.map(k =>
                Key.assertBuild(exercisedEvent.templateId, k.unversioned)
              ),
              stakeholders = exercisedEvent.flatEventWitnesses.map(Party.assertFromString),
              eventOffset = exercisedEvent.eventOffset,
              eventSequentialId = exercisedEvent.eventSequentialId,
            )
        }
      case TransactionLogUpdate.LedgerEndMarker(eventOffset, eventSequentialId) =>
        Iterator(ContractStateEvent.LedgerEndMarker(eventOffset, eventSequentialId))
    }
}
