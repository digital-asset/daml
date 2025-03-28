// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.HealthListener
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.SequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXState,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.{
  SequencerConnectionXPoolConfig,
  SequencerConnectionXPoolError,
  SequencerConnectionXPoolHealth,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, SeqUtil, SingleUseCell}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.{immutable, mutable}
import scala.concurrent.{ExecutionContextExecutor, blocking}
import scala.util.Random

class SequencerConnectionXPoolImpl private[sequencing] (
    private val initialConfig: SequencerConnectionXPoolConfig,
    connectionFactory: SequencerConnectionXFactory,
    clock: Clock,
    seedForRandomnessO: Option[Long],
    override val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectionXPool {
  import SequencerConnectionXPoolImpl.*

  /** Reference to the currently active configuration */
  private val configRef = new AtomicReference[SequencerConnectionXPoolConfig](initialConfig)

  /** Flag indicating whether the connection pool has been started */
  private val startedRef = new AtomicBoolean(false)

  /** The pool itself, as a sequence of connections per sequencer ID */
  private val pool = mutable.Map[SequencerId, NonEmpty[Seq[SequencerConnectionX]]]()

  /** Tracks the configured connections, along with their validated status. */
  private val trackedConnections: mutable.Map[SequencerConnectionX, Boolean] =
    new mutable.HashMap[SequencerConnectionX, Boolean]()

  /** Bootstrap information once the pool has been initialized */
  private val bootstrapCell = new SingleUseCell[BootstrapInfo]

  /** Used to provide randomness in connection selection */
  private val random = seedForRandomnessO.fold(new Random)(new Random(_))

  /** Used to synchronize access to the mutable structures [[pool]] and [[trackedConnections]]
    */
  private val lock = new Object()

  override def config: SequencerConnectionXPoolConfig = configRef.get

  override val health: SequencerConnectionXPoolHealth = new SequencerConnectionXPoolHealth(
    name = "sequencer-connection-pool",
    associatedHasRunOnClosing = this,
    logger = logger,
  )

  @VisibleForTesting
  override def contents: Map[SequencerId, Set[SequencerConnectionX]] = blocking {
    lock.synchronized {
      pool.view.mapValues(_.toSet).toMap
    }
  }

  override def synchronizerId: Option[SynchronizerId] = bootstrapCell.get.map(_.synchronizerId)

  override def start()(implicit
      traceContext: TraceContext
  ): Unit =
    if (startedRef.getAndSet(true)) {
      logger.debug("Connection pool already started -- ignoring")
    } else {
      updateTrackedConnections(toBeAdded = config.connections, toBeRemoved = Set.empty)
    }

  private def updateTrackedConnections(
      toBeAdded: immutable.Iterable[ConnectionXConfig],
      toBeRemoved: Set[ConnectionXConfig],
  )(implicit traceContext: TraceContext): Unit = blocking {
    lock.synchronized {
      val removedConnections =
        trackedConnections.keySet.filter(connection => toBeRemoved.contains(connection.config))
      val newConnections = toBeAdded.map(connectionFactory.create)

      trackedConnections.filterInPlace { case (connection, _) =>
        !removedConnections.contains(connection)
      }
      removedConnections.foreach { connection =>
        removeConnectionFromPool(connection)
      }
      // Newly created connections are not yet validated
      trackedConnections.addAll(newConnections.map(_ -> false))

      // Note that the following calls to `connection.fatal` and `startConnection` can potentially trigger
      // further processing via health state changes and modify the tracked connections or the pool.
      // Even if this happens in the same thread, it does not interfere with our processing.
      removedConnections.foreach { connection =>
        connection.fatal("removed from config")
      }
      // If start() or updateConfig() is called after the pool has been closed, we don't want to start new connections
      if (!isClosing) {
        newConnections.map(new ConnectionHandler(_)).foreach(_.startConnection())
      }
    }
  }

  private class ConnectionHandler(connection: SequencerConnectionX) extends NamedLogging {
    override protected val loggerFactory: NamedLoggerFactory =
      SequencerConnectionXPoolImpl.this.loggerFactory
        .append("connection", s"${connection.config.name}")

    private val restartScheduledRef: AtomicBoolean = new AtomicBoolean(false)

    private def scheduleRestart()(implicit traceContext: TraceContext): Unit =
      if (restartScheduledRef.getAndSet(true))
        logger.debug("Restart already scheduled -- ignoring")
      else {
        logger.debug("Scheduling restart")
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          clock.scheduleAfter(_ => restart(), config.restartConnectionDelay),
          s"restart-connection-${connection.name}",
        )
      }

    private def restart()(implicit traceContext: TraceContext): Unit =
      if (!isClosing) {
        if (restartScheduledRef.getAndSet(false)) {
          logger.debug("Restarting")
          startConnection()
        }
      }

    def startConnection()(implicit traceContext: TraceContext): Unit =
      connection
        .start()
        .valueOr(err => logger.warn(s"Failed to start connection ${connection.name}: $err"))

    private def register(): Unit =
      connection.health
        .registerOnHealthChange(new HealthListener {
          override def name: String = s"SequencerConnectionPool-${connection.name}"

          override def poke()(implicit traceContext: TraceContext): Unit = {
            val state = connection.health.getState

            state match {
              case SequencerConnectionXState.Validated => processValidatedConnection(connection)

              case SequencerConnectionXState.Stopped =>
                removeConnectionFromPool(connection)
                if (!isClosing) scheduleRestart()

              // For any other state, ensure the connection is not in the pool
              case SequencerConnectionXState.Fatal |
                  SequencerConnectionXState.Initial | SequencerConnectionXState.Starting |
                  SequencerConnectionXState.Started | SequencerConnectionXState.Stopping =>
                removeConnectionFromPool(connection)
            }
          }
        })
        .discard[Boolean]

    register()
  }

  override def updateConfig(
      newConfig: SequencerConnectionXPoolConfig
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionXPool.SequencerConnectionXPoolError, Unit] =
    blocking {
      lock.synchronized {
        val currentConfig = config
        val newThreshold = newConfig.trustThreshold
        val poolNotInitialized = bootstrapCell.get.isEmpty

        for {
          _ <- newConfig.validate
          _ <- Either.cond(
            newConfig.expectedSynchronizerIdO == currentConfig.expectedSynchronizerIdO,
            (),
            SequencerConnectionXPoolError.InvalidConfigurationError(
              "The expected synchronizer ID can only be changed during a node restart."
            ),
          )

          changedConnections = currentConfig.changedConnections(newConfig)

          // Check whether the new threshold is now reached, which can only happen if it was lowered
          bootstrapIfThresholdReachedO <-
            if (poolNotInitialized && newThreshold < currentConfig.trustThreshold) {
              getBootstrapIfThresholdReached(
                newThreshold,
                // Removed connections must not be counted.
                // We don't remove them yet from the tracked connections in case many bootstraps reach the
                // threshold and the config gets rejected.
                ignored = changedConnections.removed,
              )
                .leftMap(SequencerConnectionXPoolError.InvalidConfigurationError.apply)
            } else {
              Right(None)
            }
        } yield {
          configRef.set(newConfig)

          // If the trust threshold is now reached, process it
          bootstrapIfThresholdReachedO.foreach(initializePool)

          updateTrackedConnections(
            toBeAdded = changedConnections.added,
            toBeRemoved = changedConnections.removed,
          )
        }
      }
    }

  private def markValidated(connection: SequencerConnectionX): Unit =
    blocking {
      lock.synchronized {
        require(trackedConnections.contains(connection))
        trackedConnections.update(connection, true)
      }
    }

  /** Check whether a group of validated connections reaches the threshold
    *
    * @param ignored
    *   Connection configs which should be ignored for checking the threshold
    * @return
    *   Either an optional bootstrap info if the threshold is reached, or an error if more than one
    *   bootstrap info has reached the threshold.
    */
  private def getBootstrapIfThresholdReached(
      threshold: PositiveInt,
      ignored: Set[ConnectionXConfig] = Set.empty,
  ): Either[String, Option[BootstrapInfo]] = {
    // No need to synchronize here as all callers have already acquired the lock

    val attributesOfValidatedConnections = trackedConnections.toSeq.mapFilter {
      case (connection, validated) if validated && !ignored.contains(connection.config) =>
        checked(Some(connection.tryAttributes))
      case _ => None
    }.toSet
    // We build this map for every check as it makes it easier to handle adding and removing connections.
    // If this becomes a concern, we can optimize it in the future.
    val bootstrapToSequencerIds = attributesOfValidatedConnections
      .groupMap(BootstrapInfo.fromAttributes)(_.sequencerId)

    // TODO(i24790): Add alert when trust threshold cannot be reached with remaining connections

    bootstrapToSequencerIds.toSeq.mapFilter {
      case (bootstrapInfo, sequencerIds) if sequencerIds.sizeIs >= threshold.unwrap =>
        Some(bootstrapInfo)
      case _ => None
    } match {
      case Seq() => Right(None)
      case Seq(bootstrapInfo) => Right(Some(bootstrapInfo))
      case many =>
        Left(
          s"Trust threshold of $threshold is reached with multiple bootstrap infos: $many." +
            s" Please configure a higher trust threshold."
        )
    }
  }

  private def initializePool(
      bootstrapInfo: BootstrapInfo
  )(implicit traceContext: TraceContext): Unit =
    // No need to synchronize here as all callers have already acquired the lock
    bootstrapCell.putIfAbsent(bootstrapInfo) match {
      case None =>
        logger.debug(
          s"Trust threshold of ${config.trustThreshold} reached with $bootstrapInfo"
        )
        trackedConnections.foreach {
          case (connection, validated) if validated =>
            val attributes = checked(connection.tryAttributes)
            val bootstrapInfoFromConnection = BootstrapInfo.fromAttributes(attributes)

            if (bootstrapInfoFromConnection == bootstrapInfo) {
              addConnectionToPool(connection, attributes.sequencerId)
            } else {
              invalidBootstrap(connection, bootstrapInfo, bootstrapInfoFromConnection)
            }
          case _ =>
        }

      case _ => ErrorUtil.invalidState("The pool has already been initialized")
    }

  override def nbSequencers: NonNegativeInt = blocking {
    lock.synchronized(NonNegativeInt.tryCreate(pool.size))
  }

  override def nbConnections: NonNegativeInt = blocking {
    lock.synchronized(NonNegativeInt.tryCreate(pool.values.flatten.size))
  }

  override def onClosed(): Unit = {
    val instances = blocking {
      lock.synchronized {
        trackedConnections.keySet.toSeq
      }
    }
    // We close the connections outside the critical section to avoid shutdown problems in case
    // it triggers health callbacks
    LifeCycle.close(instances*)(logger)
    super.onClosed()
  }

  private def processValidatedConnection(connection: SequencerConnectionX)(implicit
      traceContext: TraceContext
  ): Unit = blocking {
    lock.synchronized {
      if (isClosing) connection.fatal("closing due to shutdown")
      else {
        val currentConfig = config
        val attributes = checked(connection.tryAttributes)
        val bootstrapInfo = BootstrapInfo.fromAttributes(attributes)

        logger.debug(s"Processing validated connection ${connection.name} with $bootstrapInfo")

        bootstrapCell.get match {
          case None =>
            if (currentConfig.expectedSynchronizerIdO.forall(_ == attributes.synchronizerId)) {
              markValidated(connection)
              getBootstrapIfThresholdReached(currentConfig.trustThreshold)
                .valueOr(ErrorUtil.invalidState(_)) // We cannot reach the threshold multiple times
                .fold(
                  logger.debug(
                    s"Trust threshold of ${currentConfig.trustThreshold} not yet reached"
                  )
                )(initializePool)
            } else {
              logger.warn(
                s"Connection ${connection.name} is not on expected synchronizer:" +
                  s" expected ${currentConfig.expectedSynchronizerIdO}," +
                  s" got ${attributes.synchronizerId}. Closing connection."
              )
              connection.fatal(reason = "invalid synchronizer")
            }

          case Some(`bootstrapInfo`) =>
            addConnectionToPool(connection, attributes.sequencerId)

          case Some(currentBootstrapInfo) =>
            invalidBootstrap(connection, currentBootstrapInfo, bootstrapInfo)
        }
      }
    }
  }

  private def invalidBootstrap(
      connection: SequencerConnectionX,
      good: BootstrapInfo,
      bad: BootstrapInfo,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.warn(
      s"Connection ${connection.name} has invalid bootstrap info: expected $good, got $bad. Closing connection."
    )
    connection.fatal(reason = "invalid bootstrap info")
  }

  private def addConnectionToPool(
      connection: SequencerConnectionX,
      sequencerId: SequencerId,
  )(implicit traceContext: TraceContext): Unit = blocking {
    lock.synchronized {
      logger.debug(s"Adding ${connection.name} to the pool")

      pool
        .updateWith(sequencerId) {
          case None => Some(NonEmpty.mk(Seq, connection))
          case Some(current) if current.contains(connection) =>
            // Can happen if a connection is poked several times with the 'validated' state
            logger.debug(
              s"Connection ${connection.name} is already present in the pool for $sequencerId"
            )
            Some(current)
          case Some(current) =>
            Some(current :+ connection)
        }
        .discard

      updateHealth()
    }
  }

  private def removeConnectionFromPool(
      connection: SequencerConnectionX
  )(implicit traceContext: TraceContext): Unit = blocking {
    lock.synchronized {
      connection.attributes match {
        case Some(attributes) =>
          val sequencerId = attributes.sequencerId

          pool
            .updateWith(sequencerId) {
              case Some(current) if current.contains(connection) =>
                logger.debug(s"Removing $connection from the pool")
                val newList = current.filter(_ != connection)
                NonEmpty.from(newList)
              case None => None
              case Some(current) => Some(current)
            }
            .discard

          updateHealth()

        case None =>
        // The connection has never been validated, and therefore cannot be in the pool
      }
    }
  }

  private def updateHealth()(implicit traceContext: TraceContext): Unit = {
    val trustThreshold = config.trustThreshold
    nbSequencers match {
      case nb if nb >= trustThreshold => health.resolveUnhealthy()
      case nb if nb > NonNegativeInt.zero =>
        health.degradationOccurred(
          s"only $nb connection(s) to different sequencers available, trust threshold = $trustThreshold"
        )
      case _ => health.failureOccurred("no connection available")
    }
  }

  override def getConnections(
      requestedNumber: Int,
      exclusions: Set[SequencerId],
  )(implicit traceContext: TraceContext): Set[SequencerConnectionX] =
    // Always return connections randomly for now
    blocking {
      lock.synchronized {
        logger.debug(
          s"Requesting $requestedNumber connections excluding ${exclusions.map(_.uid.identifier)}"
        )

        // Pick up to `requestedNumber` non-excluded sequencer IDs from the pool
        val randomSeqIds = SeqUtil.randomSubsetShuffle(
          pool.keySet.diff(exclusions).toIndexedSeq,
          requestedNumber,
          random,
        )

        val result = randomSeqIds.map { seqId =>
          val connections = pool(seqId) // seqIDs were picked from the pool so this cannot fail

          // Cheap round-robin: return the head connection and move it to the end
          val head = connections.head1
          val newList = NonEmpty.from(connections.tail1) match {
            case None => NonEmpty.mk(Seq, head)
            case Some(nonEmptyTail) => nonEmptyTail :+ head
          }
          pool.update(seqId, newList)

          head
        }.toSet

        logger.debug(s"Returning ${result.map(_.config.name)}")
        result
      }
    }
}

object SequencerConnectionXPoolImpl {
  private final case class BootstrapInfo private (
      synchronizerId: SynchronizerId,
      staticParameters: StaticSynchronizerParameters,
  )

  private object BootstrapInfo {
    def fromAttributes(attributes: ConnectionAttributes): BootstrapInfo =
      BootstrapInfo(attributes.synchronizerId, attributes.staticParameters)
  }
}
