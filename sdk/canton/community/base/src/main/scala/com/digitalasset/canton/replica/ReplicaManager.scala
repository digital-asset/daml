// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.replica

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.store.EncryptedCryptoPrivateStore
import com.digitalasset.canton.error.FatalError
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  CloseContext,
  FlagCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.{
  EitherTUtil,
  FutureUnlessShutdownUtil,
  SimpleExecutionQueue,
  SingleUseCell,
}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

abstract class ReplicaManager(
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync { self =>

  private val execQueue = new SimpleExecutionQueue(
    "replica-manager-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  private def makeCloseContext: FlagCloseable = FlagCloseable(logger, timeouts)

  protected val sessionCloseContext: AtomicReference[FlagCloseable] =
    new AtomicReference[FlagCloseable](makeCloseContext)

  def getSessionContext: CloseContext = CloseContext(sessionCloseContext.get())

  private val replicaStateRef: AtomicReference[Option[ReplicaState]] =
    new AtomicReference[Option[ReplicaState]](None)

  private val privateKeyStoreRef: SingleUseCell[EncryptedCryptoPrivateStore] =
    new SingleUseCell[EncryptedCryptoPrivateStore]

  protected def transitionToActive()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  protected def transitionToPassive()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  private def setState(newState: ReplicaState)(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Setting replica manager state to $newState")
    replicaStateRef.set(Some(newState))
  }

  private def changeState[A](newState: ReplicaState, setNewStateEagerly: Boolean)(
      body: => FutureUnlessShutdown[A]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[A]] = {
    logger.info(s"Transitioning replica state to $newState")
    execQueue.executeUS(
      synchronizeWithClosing(functionFullName) {
        if (replicaStateRef.get().contains(newState)) {
          logger.debug(s"Replica already in state $newState, ignoring replica state change")
          FutureUnlessShutdown.pure(None)
        } else {
          if (setNewStateEagerly) {
            // Set the state to transition from one state to another before running the body
            logger.info(s"Eagerly setting replica state to $newState")
            setState(newState)
          }

          FutureUnlessShutdownUtil
            .logOnFailureUnlessShutdown(
              body,
              s"Failed to run replica state transition to $newState",
            )
            .map { res =>
              if (!setNewStateEagerly)
                setState(newState)
              logger.info(s"Successfully performed replica state change to $newState")
              Some(res)
            }
        }
      },
      s"replica state update to $newState",
    )
  }

  def setInitialState(state: ReplicaState): Unit = {
    noTracingLogger.info(s"Setting initial replica state to $state")
    if (!replicaStateRef.compareAndSet(None, Some(state)))
      noTracingLogger.info(s"Failed to set initial state to $state, was already initialized")
  }

  def setPrivateKeyStore(privateKeyStore: EncryptedCryptoPrivateStore): Unit =
    privateKeyStoreRef.putIfAbsent(privateKeyStore).foreach { _ =>
      logger.warn(s"Failed to set initial private key store, was already initialized")(
        TraceContext.empty
      )
    }

  def setActive(): FutureUnlessShutdown[Unit] = withNewTraceContext("active_replica") {
    implicit traceContext =>
      changeState(ReplicaState.Active, setNewStateEagerly = false) {
        for {
          _ <- privateKeyStoreRef.get.fold(FutureUnlessShutdown.unit) { store =>
            EitherTUtil.toFutureUnlessShutdown(
              store
                .refreshWrapperKey()
                .leftMap(err => new RuntimeException(s"Failed to refresh wrapper key: $err"))
            )
          }
          _ <- transitionToActive()
        } yield ()
      }
        .map(_ => ())
        .recover {
          case exception if exitOnFatalFailures =>
            FatalError.exitOnFatalError("Failed to transition node to active", exception, logger)
        }
  }

  def setPassive(): FutureUnlessShutdown[Option[CloseContext]] =
    withNewTraceContext("passive_replica") { implicit traceContext =>
      // Close the session context first but then don't wait for the close to be done before transitioning to passive.
      // Once transition is complete, make sure the session context has closed.
      // That is because the session context is used in storage to perform DB queries.
      // Therefore closing it will mean waiting for all DB operations to complete.
      // For that to happen we need to also start the passive transition otherwise the node will keep functioning normally
      // and try to perform more DB queries.
      // But we also want the session context to be closing while we transition to passive to stop any DB retries.
      changeState(ReplicaState.Passive, setNewStateEagerly = true) {
        val newContext = makeCloseContext
        val oldContext = sessionCloseContext.getAndSet(newContext)
        val sessionContextIsClosed =
          FutureUnlessShutdown.outcomeF(Future(oldContext.close()))
        logger.debug("Starting transition to passive")
        for {
          _ <- transitionToPassive()
          _ = logger.debug("Waiting for session context to be closed")
          _ <- sessionContextIsClosed
        } yield CloseContext(newContext)
      }.recover {
        case exception if exitOnFatalFailures =>
          FatalError.exitOnFatalError("Failed to transition node to passive", exception, logger)
      }
    }

  def isActive: Boolean = replicaStateRef.get().contains(ReplicaState.Active)

  override def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq(
      SyncCloseable("closeQueue", execQueue.close()),
      SyncCloseable("closeInternal", closeInternal()),
    )

  protected def closeInternal(): Unit = ()
}

sealed trait ReplicaState extends Product with Serializable
object ReplicaState {
  case object Active extends ReplicaState
  case object Passive extends ReplicaState
}
