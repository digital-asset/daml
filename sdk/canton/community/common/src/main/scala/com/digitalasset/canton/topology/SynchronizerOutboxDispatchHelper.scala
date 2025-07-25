// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.RegisterTopologyTransactionHandle
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  RunOnClosing,
}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

trait SynchronizerOutboxDispatchHelper extends NamedLogging {
  protected def psid: PhysicalSynchronizerId

  protected def memberId: Member

  final protected def protocolVersion: ProtocolVersion = psid.protocolVersion

  protected def crypto: SynchronizerCrypto

  protected def convertTransactions(transactions: Seq[GenericSignedTopologyTransaction])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[GenericSignedTopologyTransaction]]

  protected def filterTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      predicate: GenericSignedTopologyTransaction => FutureUnlessShutdown[Boolean],
  )(implicit
      executionContext: ExecutionContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    transactions.parFilterA(tx => predicate(tx))

  protected def topologyTransaction(
      tx: GenericSignedTopologyTransaction
  ): PrettyPrinting = tx.transaction

  protected def onlyApplicable(
      transactions: Seq[GenericSignedTopologyTransaction]
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    FutureUnlessShutdown.pure(
      transactions.filter(x => x.mapping.restrictedToSynchronizer.forall(_ == psid.logical))
    )

  protected def isFailedState(response: TopologyTransactionsBroadcast.State): Boolean =
    response == TopologyTransactionsBroadcast.State.Failed

  def isExpectedState(state: TopologyTransactionsBroadcast.State): Boolean = state match {
    case TopologyTransactionsBroadcast.State.Failed => false
    case TopologyTransactionsBroadcast.State.Accepted => true
  }
}

trait StoreBasedSynchronizerOutboxDispatchHelper extends SynchronizerOutboxDispatchHelper {

  def authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore]

  override protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, /*SynchronizerRegistryError*/ String, Seq[
    GenericSignedTopologyTransaction
  ]] =
    transactions
      .parTraverse { tx =>
        if (tx.transaction.isEquivalentTo(protocolVersion)) {
          // Transaction already in the correct version, nothing to do here
          EitherT.rightT[FutureUnlessShutdown, String](tx)
        } else {
          // First try to find if the topology transaction already exists in the correct version in the topology store
          OptionT(
            authorizedStore.findStoredForVersion(
              CantonTimestamp.MaxValue,
              tx.transaction,
              protocolVersion,
            )
          )
            .map(_.transaction)
            .toRight("")
            .leftFlatMap { _ =>
              // We did not find a topology transaction with the correct version, so we try to convert and resign
              SignedTopologyTransaction
                .asVersion(tx, protocolVersion)(crypto)
            }
        }
      }

}

trait QueueBasedSynchronizerOutboxDispatchHelper extends SynchronizerOutboxDispatchHelper {
  override protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, /*SynchronizerRegistryError*/ String, Seq[
    GenericSignedTopologyTransaction
  ]] =
    transactions
      .parTraverse { tx =>
        if (tx.transaction.isEquivalentTo(protocolVersion)) {
          // Transaction already in the correct version, nothing to do here
          EitherT.rightT[FutureUnlessShutdown, String](tx)
        } else {
          SignedTopologyTransaction
            .asVersion(tx, protocolVersion)(crypto)
        }
      }
}

trait SynchronizerOutboxDispatch extends NamedLogging with FlagCloseable {
  this: SynchronizerOutboxDispatchHelper =>

  protected def targetStore: TopologyStore[TopologyStoreId.SynchronizerStore]
  protected def handle: RegisterTopologyTransactionHandle

  // register handle close task
  // this will ensure that the handle is closed before the outbox, aborting any retries
  runOnOrAfterClose_(new RunOnClosing {
    override def name: String = "close-handle"
    override def done: Boolean = handle.isClosing
    override def run()(implicit traceContext: TraceContext): Unit = LifeCycle.close(handle)(logger)
  })(TraceContext.empty)

  protected def notAlreadyPresent(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    val doesNotAlreadyExistPredicate = (tx: GenericSignedTopologyTransaction) =>
      targetStore.providesAdditionalSignatures(tx)
    filterTransactions(transactions, doesNotAlreadyExistPredicate)
  }

  protected def dispatch(
      synchronizerAlias: SynchronizerAlias,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[TopologyTransactionsBroadcast.State]] =
    if (transactions.isEmpty) EitherT.rightT(Seq.empty)
    else {
      implicit val success = retry.Success.always
      val ret = retry
        .Backoff(
          logger,
          this,
          timeouts.unbounded.retries(1.second),
          1.second,
          10.seconds,
          "push topology transaction",
        )
        .unlessShutdown(
          {
            logger.debug(
              s"Attempting to push ${transactions.size} topology transactions to $synchronizerAlias: $transactions"
            )
            FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
              handle.submit(transactions),
              s"Pushing topology transactions to $synchronizerAlias",
            )
          },
          AllExceptionRetryPolicy,
        )
        .map { responses =>
          if (responses.sizeCompare(transactions) != 0) {
            logger.error(
              s"Topology request contained ${transactions.length} txs, but I received responses for ${responses.length}"
            )
          }
          logger.debug(
            s"$synchronizerAlias responded the following for the given topology transactions: $responses"
          )
          val failedResponses =
            responses.zip(transactions).collect {
              case (response, tx) if isFailedState(response) => tx
            }

          Either.cond(
            failedResponses.isEmpty,
            responses,
            s"The synchronizer $synchronizerAlias failed the following topology transactions: $failedResponses",
          )
        }
      EitherT(
        ret
      )
    }
}
