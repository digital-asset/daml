// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.RegisterTopologyTransactionHandleCommon
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  RunOnShutdown,
}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreCommon, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{OwnerToKeyMapping, SignedTopologyTransaction}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.{FutureUtil, retry}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

trait DomainOutboxDispatchStoreSpecific[TX, State] extends NamedLogging {
  protected def domainId: DomainId
  protected def memberId: Member
  protected def protocolVersion: ProtocolVersion
  protected def crypto: Crypto

  protected def topologyTransaction(tx: TX): PrettyPrinting

  protected def filterTransactions(
      transactions: Seq[TX],
      predicate: TX => Future[Boolean],
  )(implicit executionContext: ExecutionContext): Future[Seq[TX]]

  protected def onlyApplicable(transactions: Seq[TX]): Future[Seq[TX]]

  protected def convertTransactions(transactions: Seq[TX])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, Seq[TX]]

  protected def isFailedState(response: State): Boolean

  protected def isExpectedState(state: State): Boolean

}

trait DomainOutboxDispatchHelperOld
    extends DomainOutboxDispatchStoreSpecific[
      GenericSignedTopologyTransaction,
      RegisterTopologyTransactionResponseResult.State,
    ] {
  def authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore]

  override protected def filterTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      predicate: GenericSignedTopologyTransaction => Future[Boolean],
  )(implicit
      executionContext: ExecutionContext
  ): Future[Seq[GenericSignedTopologyTransaction]] =
    transactions.parFilterA(tx => predicate(tx))

  override protected def topologyTransaction(
      tx: GenericSignedTopologyTransaction
  ): PrettyPrinting = tx.transaction

  protected def onlyApplicable(
      transactions: Seq[GenericSignedTopologyTransaction]
  ): Future[Seq[GenericSignedTopologyTransaction]] = {
    def notAlien(tx: GenericSignedTopologyTransaction): Boolean = {
      val mapping = tx.transaction.element.mapping
      mapping match {
        case OwnerToKeyMapping(_: ParticipantId, _) => true
        case OwnerToKeyMapping(owner, _) => owner.uid == domainId.unwrap
        case _ => true
      }
    }

    def domainRestriction(tx: GenericSignedTopologyTransaction): Boolean =
      tx.transaction.element.mapping.restrictedToDomain.forall(_ == domainId)

    Future.successful(
      transactions.filter(x => notAlien(x) && domainRestriction(x))
    )
  }

  override protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, /*DomainRegistryError*/ String, Seq[GenericSignedTopologyTransaction]] = {
    transactions
      .parTraverse { tx =>
        if (tx.transaction.hasEquivalentVersion(protocolVersion)) {
          // Transaction already in the correct version, nothing to do here
          EitherT.rightT[Future, String](tx)
        } else {
          // First try to find if the topology transaction already exists in the correct version in the topology store
          OptionT(authorizedStore.findStoredForVersion(tx.transaction, protocolVersion))
            .map(_.transaction)
            .toRight("")
            .leftFlatMap { _ =>
              // We did not find a topology transaction with the correct version, so we try to convert and resign
              SignedTopologyTransaction.asVersion(tx, protocolVersion)(crypto)
            }
        }
      }
  }

  override protected def isFailedState(
      response: RegisterTopologyTransactionResponseResult.State
  ): Boolean = response == RegisterTopologyTransactionResponseResult.State.Failed

  override def isExpectedState(state: RegisterTopologyTransactionResponseResult.State): Boolean =
    state match {
      case RegisterTopologyTransactionResponseResult.State.Requested => false
      case RegisterTopologyTransactionResponseResult.State.Failed => false
      case RegisterTopologyTransactionResponseResult.State.Rejected => false
      case RegisterTopologyTransactionResponseResult.State.Accepted => true
      case RegisterTopologyTransactionResponseResult.State.Duplicate => true
      case RegisterTopologyTransactionResponseResult.State.Obsolete => true
    }
}

trait DomainOutboxDispatch[
    TX,
    State,
    +H <: RegisterTopologyTransactionHandleCommon[TX, State],
    +TS <: TopologyStoreCommon[TopologyStoreId.DomainStore, ?, ?, TX],
] extends NamedLogging
    with FlagCloseable { this: DomainOutboxDispatchStoreSpecific[TX, State] =>

  protected def targetStore: TS
  protected def handle: H

  // register handle close task
  // this will ensure that the handle is closed before the outbox, aborting any retries
  runOnShutdown_(new RunOnShutdown {
    override def name: String = "close-handle"
    override def done: Boolean = handle.isClosing
    override def run(): Unit = Lifecycle.close(handle)(logger)
  })(TraceContext.empty)

  protected def notAlreadyPresent(
      transactions: Seq[TX]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Seq[TX]] = {
    val doesNotAlreadyExistPredicate = (tx: TX) => targetStore.providesAdditionalSignatures(tx)
    filterTransactions(transactions, doesNotAlreadyExistPredicate)
  }

  protected def dispatch(
      domain: DomainAlias,
      transactions: Seq[TX],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[State]] =
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
          AllExnRetryable,
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
