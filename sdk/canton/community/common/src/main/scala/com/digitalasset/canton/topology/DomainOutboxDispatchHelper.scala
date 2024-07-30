// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.RegisterTopologyTransactionHandle
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  RunOnShutdown,
}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.util.{FutureUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

trait DomainOutboxDispatchHelper extends NamedLogging {
  protected def domainId: DomainId

  protected def memberId: Member

  protected def protocolVersion: ProtocolVersion

  protected def crypto: Crypto

  protected def convertTransactions(transactions: Seq[GenericSignedTopologyTransaction])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[GenericSignedTopologyTransaction]]

  protected def filterTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      predicate: GenericSignedTopologyTransaction => Future[Boolean],
  )(implicit
      executionContext: ExecutionContext
  ): Future[Seq[GenericSignedTopologyTransaction]] =
    transactions.parFilterA(tx => predicate(tx))

  protected def topologyTransaction(
      tx: GenericSignedTopologyTransaction
  ): PrettyPrinting = tx.transaction

  protected def onlyApplicable(
      transactions: Seq[GenericSignedTopologyTransaction]
  ): Future[Seq[GenericSignedTopologyTransaction]] =
    Future.successful(
      transactions.filter(x => x.mapping.restrictedToDomain.forall(_ == domainId))
    )

  protected def isFailedState(response: TopologyTransactionsBroadcast.State): Boolean =
    response == TopologyTransactionsBroadcast.State.Failed

  def isExpectedState(state: TopologyTransactionsBroadcast.State): Boolean = state match {
    case TopologyTransactionsBroadcast.State.Failed => false
    case TopologyTransactionsBroadcast.State.Accepted => true
  }
}

trait StoreBasedDomainOutboxDispatchHelper extends DomainOutboxDispatchHelper {

  def authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore]
  override protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, /*DomainRegistryError*/ String, Seq[
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
          ).mapK(FutureUnlessShutdown.outcomeK)
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

trait QueueBasedDomainOutboxDispatchHelper extends DomainOutboxDispatchHelper {
  override protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, /*DomainRegistryError*/ String, Seq[
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

trait DomainOutboxDispatch extends NamedLogging with FlagCloseable {
  this: DomainOutboxDispatchHelper =>

  protected def targetStore: TopologyStore[TopologyStoreId.DomainStore]
  protected def handle: RegisterTopologyTransactionHandle

  // register handle close task
  // this will ensure that the handle is closed before the outbox, aborting any retries
  runOnShutdown_(new RunOnShutdown {
    override def name: String = "close-handle"
    override def done: Boolean = handle.isClosing
    override def run(): Unit = Lifecycle.close(handle)(logger)
  })(TraceContext.empty)

  protected def notAlreadyPresent(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Seq[GenericSignedTopologyTransaction]] = {
    val doesNotAlreadyExistPredicate = (tx: GenericSignedTopologyTransaction) =>
      targetStore.providesAdditionalSignatures(tx)
    filterTransactions(transactions, doesNotAlreadyExistPredicate)
  }

  protected def dispatch(
      domain: DomainAlias,
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
              s"Attempting to push ${transactions.size} topology transactions to $domain: $transactions"
            )
            FutureUtil.logOnFailureUnlessShutdown(
              handle.submit(transactions),
              s"Pushing topology transactions to $domain",
            )
          },
          AllExceptionRetryPolicy,
        )
        .map { responses =>
          if (responses.length != transactions.length) {
            logger.error(
              s"Topology request contained ${transactions.length} txs, but I received responses for ${responses.length}"
            )
          }
          logger.debug(
            s"$domain responded the following for the given topology transactions: $responses"
          )
          val failedResponses =
            responses.zip(transactions).collect {
              case (response, tx) if isFailedState(response) => tx
            }

          Either.cond(
            failedResponses.isEmpty,
            responses,
            s"The domain $domain failed the following topology transactions: $failedResponses",
          )
        }
      EitherT(
        ret
      )
    }
}
