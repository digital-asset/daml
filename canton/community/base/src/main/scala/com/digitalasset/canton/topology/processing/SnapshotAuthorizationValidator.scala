// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.TransactionAuthorizationValidator.AuthorizationChain
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{MonadUtil, SimpleExecutionQueue}

import scala.concurrent.{ExecutionContext, Future}

/** Compute the authorization chain for a certain UID */
class SnapshotAuthorizationValidator(
    asOf: CantonTimestamp,
    val store: TopologyStore[TopologyStoreId],
    override val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit executionContext: ExecutionContext)
    extends TransactionAuthorizationValidator
    with NamedLogging
    with FlagCloseable {

  private val sequential = new SimpleExecutionQueue(
    "snapshot-authorization-validator-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  def authorizedBy(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AuthorizationChain]] = {
    // preload our cache. note, we don't want to load stuff into our cache concurrently, so we
    // squeeze this through a sequential execution queue
    val preloadF = transaction.transaction.element.mapping.requiredAuth match {
      case RequiredAuth.Ns(namespace, _) =>
        sequential.execute(
          loadAuthorizationGraphs(
            asOf,
            Set(namespace),
          ),
          functionFullName,
        )
      case RequiredAuth.Uid(uids) =>
        sequential.execute(
          {
            val graphF = loadAuthorizationGraphs(asOf, uids.map(_.namespace).toSet)
            val delF = loadIdentifierDelegations(asOf, Seq.empty, uids.toSet)
            graphF.zip(delF)
          },
          functionFullName,
        )
    }

    preloadF.map { _ =>
      authorizationChainFor(transaction)
    }
  }

  def removeNamespaceDelegationFromCache(
      namespace: Namespace,
      nsd: StoredTopologyTransactions[TopologyChangeOp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    sequential.execute(
      Future {
        namespaceCache
          .get(namespace)
          .fold(())(ag =>
            ag.unauthorizedRemove(nsd.toAuthorizedTopologyTransactions {
              case x: NamespaceDelegation => x
            })
          )
      },
      functionFullName,
    )

  def removeIdentifierDelegationFromCache(
      uid: UniqueIdentifier,
      nsd: StoredTopologyTransactions[TopologyChangeOp],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    sequential.execute(
      Future {
        val authorizedNsd = nsd.toAuthorizedTopologyTransactions { case x: IdentifierDelegation =>
          x
        }
        updateIdentifierDelegationCache(uid, { _.filterNot(Seq(_) == authorizedNsd) })
      },
      functionFullName,
    )

  def reset()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    sequential.execute(
      Future {
        identifierDelegationCache.clear()
        namespaceCache.clear()
      },
      functionFullName,
    )

  override protected def onClosed(): Unit = Lifecycle.close {
    sequential
  }(logger)

}

object SnapshotAuthorizationValidator {

  def validateTransactions(
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]]
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, TopologyStore[AuthorizedStore]] = {
    val store =
      new InMemoryTopologyStore(
        AuthorizedStore,
        loggerFactory,
        timeouts,
        futureSupervisor,
      )
    val validator =
      new SnapshotAuthorizationValidator(
        CantonTimestamp.MaxValue,
        store,
        timeouts,
        loggerFactory,
        futureSupervisor,
      )

    def requiresReset(tx: SignedTopologyTransaction[TopologyChangeOp]): Boolean =
      tx.transaction.element.mapping.dbType == DomainTopologyTransactionType.NamespaceDelegation ||
        tx.transaction.element.mapping.dbType == DomainTopologyTransactionType.IdentifierDelegation

    // check that all transactions are authorized
    val tmp: EitherT[FutureUnlessShutdown, String, Unit] = EitherT(
      MonadUtil.foldLeftM(Right(()): Either[String, Unit], transactions.zipWithIndex) {
        case (Right(_), (tx, idx)) =>
          val ts = CantonTimestamp.Epoch.plusMillis(idx.toLong)
          // incrementally add it to the store and check the validation
          for {
            isValidated <- validator.authorizedBy(tx).map(_.nonEmpty)
            _ <- FutureUnlessShutdown.outcomeF(
              store.append(
                SequencedTime(ts),
                EffectiveTime(ts),
                Seq(ValidatedTopologyTransaction(tx, None)),
              )
            )
            // if the transaction was a namespace delegation, drop it
            _ <- if (requiresReset(tx)) validator.reset() else FutureUnlessShutdown.unit
          } yield Either.cond(
            isValidated,
            (),
            s"Unauthorized topology transaction: $tx",
          )
        case (acc, _) => FutureUnlessShutdown.pure(acc)
      }
    )
    tmp.map(_ => store)
  }

}
