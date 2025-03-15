// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{AtomicHealthElement, CompositeHealthElement, HealthListener}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasRunOnClosing, LifeCycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencing.ConnectionX.{ConnectionXConfig, ConnectionXState}
import com.digitalasset.canton.sequencing.SequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXError,
  SequencerConnectionXHealth,
  SequencerConnectionXInternalError,
  SequencerConnectionXState,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXClient.SequencerConnectionXClientError
import com.digitalasset.canton.sequencing.client.transports.GrpcClientTransportHelpers
import com.digitalasset.canton.sequencing.protocol.HandshakeResponse
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, blocking}
import scala.util.Failure

/** Sequencer connection specialized for gRPC transport.
  */
class GrpcSequencerConnectionX private (
    config: ConnectionXConfig,
    clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
    minimumProtocolVersion: Option[ProtocolVersion],
    clientFactory: SequencerConnectionXClientFactory,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectionX
    with PrettyPrinting
    with GrpcClientTransportHelpers {
  import GrpcSequencerConnectionX.*

  private val connection: ConnectionX = GrpcConnectionX(config, timeouts, loggerFactory)
  private val client: SequencerConnectionXClient = clientFactory.create(connection)
  private val attributesCell = new SingleUseCell[ConnectionAttributes]
  private val localState = new AtomicReference[LocalState](LocalState.Initial)

  // The sequencer connection health state is determined by the underlying connection state and the local state
  override val health: GrpcSequencerConnectionXHealth = new GrpcSequencerConnectionXHealth(
    name = name,
    associatedHasRunOnClosing = this,
    logger = logger,
  ) with CompositeHealthElement[String, AtomicHealthElement] {

    setDependency("connection", connection.health)

    override protected def combineDependentStates: SequencerConnectionXState =
      (connection.health.getState, localState.get) match {
        case (_, LocalState.Fatal) => SequencerConnectionXState.Fatal
        case (ConnectionXState.Stopped, LocalState.Starting) => SequencerConnectionXState.Starting
        case (ConnectionXState.Stopped, _) => SequencerConnectionXState.Stopped
        case (ConnectionXState.Started, LocalState.Stopping) => SequencerConnectionXState.Stopping
        case (ConnectionXState.Started, LocalState.Validated) => SequencerConnectionXState.Validated
        case (ConnectionXState.Started, _) => SequencerConnectionXState.Started
      }

    override def refresh()(implicit traceContext: TraceContext): Unit = refreshFromDependencies()
  }

  private val validationLimiter = new ConnectionValidationLimiter(
    validate()(_: TraceContext),
    futureSupervisor,
    loggerFactory,
  )

  health
    .registerOnHealthChange(new HealthListener {
      override def name: String = s"GrpcSequencerConnectionX-${GrpcSequencerConnectionX.this.name}"

      override def poke()(implicit traceContext: TraceContext): Unit = {
        val state = health.getState

        state match {
          case SequencerConnectionXState.Started =>
            FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
              performUnlessClosingUSF(s"validate connection ${GrpcSequencerConnectionX.this.name}")(
                validationLimiter.maybeValidate().thereafter {
                  case Failure(t) =>
                    // If a validation throws, we consider this a fatal failure of the connection
                    fatal(reason = s"Validation failed with an exception: $t")
                  case _ =>
                }
              ),
              s"validate connection ${GrpcSequencerConnectionX.this.name}",
            )

          case _ => // No action
        }
      }

    })
    .discard[Boolean]

  override def name: String = s"sequencer-${connection.name}"

  override def attributes: Option[ConnectionAttributes] = attributesCell.get

  /** Atomically update the local state using the provided function, and return the previous state.
    * Automatically enforces that we cannot recover from the `Fatal` state. This function also
    * triggers a health refresh.
    */
  private def updateLocalState(
      update: NonFatalLocalState => LocalState
  )(implicit traceContext: TraceContext): LocalState = {
    val oldState = localState.getAndUpdate {
      // Cannot recover from fatal
      case LocalState.Fatal => LocalState.Fatal
      case nonFatal: NonFatalLocalState => update(nonFatal)
    }
    health.refresh()
    oldState
  }

  override def start()(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionXError, Unit] = blocking {
    synchronized {
      logger.info(s"Starting $name")
      val oldState = updateLocalState {
        // No need to trigger a new validation
        case LocalState.Validated => LocalState.Validated
        case LocalState.Initial | LocalState.Stopping | LocalState.Starting => LocalState.Starting
      }

      oldState match {
        case LocalState.Fatal =>
          val message = "The connection is in a fatal state and cannot be started"
          logger.info(message)
          Left(SequencerConnectionXError.InvalidStateError(message))
        case LocalState.Starting | LocalState.Validated =>
          logger.debug(s"Ignoring start of connection as it is in state $oldState")
          Either.unit
        case LocalState.Initial | LocalState.Stopping =>
          connection.start()
          Either.unit
      }
    }
  }

  override def fail(reason: String)(implicit traceContext: TraceContext): Unit = blocking {
    synchronized {
      logger.info(s"Stopping $name for non-fatal reason: $reason")
      val oldState = updateLocalState {
        case LocalState.Initial => LocalState.Initial
        case LocalState.Starting | LocalState.Stopping | LocalState.Validated => LocalState.Stopping
      }

      oldState match {
        case LocalState.Fatal =>
          logger.info("The connection is in a fatal state")
        case LocalState.Initial | LocalState.Stopping =>
          logger.debug(s"Ignoring stop of connection as it is in state $oldState")
        case LocalState.Starting | LocalState.Validated =>
          connection.stop()
      }
    }
  }

  override def fatal(reason: String)(implicit traceContext: TraceContext): Unit = blocking {
    synchronized {
      logger.info(s"Stopping $name for fatal reason: $reason")
      val oldState = updateLocalState(_ => LocalState.Fatal)

      oldState match {
        case LocalState.Fatal | LocalState.Initial | LocalState.Stopping =>
          logger.debug(s"Ignoring stop of connection as it is in state $oldState")
        case LocalState.Starting | LocalState.Validated =>
          connection.stop()
      }
    }
  }

  private def validate()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def handleFailedValidation(error: SequencerConnectionXInternalError): Unit = error match {
      case SequencerConnectionXInternalError.ValidationError(message) =>
        logger.warn(s"Validation failure: $message")
        fatal("Failed validation")

      case SequencerConnectionXInternalError.ClientError(
            SequencerConnectionXClientError.DeserializationError(error)
          ) =>
        logger.info(s"Deserialization error: $error")
        fatal("Deserialization error")

      case SequencerConnectionXInternalError.ClientError(
            SequencerConnectionXClientError.ConnectionError(error)
          ) =>
        // Might need to refine on the errors
        logger.debug(s"Network error: $error")
        fail("Network error")
    }

    def handleSuccessfulValidation(newAttributes: ConnectionAttributes): Unit =
      attributesCell.putIfAbsent(newAttributes) match {
        case None | Some(`newAttributes`) =>
          logger.debug(s"Valid sequencer connection: attributes = $attributes")

          val oldLocalState = updateLocalState {
            case LocalState.Starting | LocalState.Validated => LocalState.Validated
            // Validation is void if stopping or the connection was never started
            case state @ (LocalState.Initial | LocalState.Stopping) => state
          }

          oldLocalState match {
            case LocalState.Starting | LocalState.Validated =>
            case _ =>
              logger.debug(s"Ignoring successful validation due to current state: $oldLocalState")
          }

        case Some(currentAttributes) =>
          logger.warn(
            s"Sequencer connection has changed attributes: expected $currentAttributes, got $newAttributes. Closing connection."
          )
          fatal("Attributes mismatch")
      }

    logger.debug(s"Starting validation of $name")

    val resultET = for {
      apiName <- client
        .getApiName(retryPolicy = retryPolicy(retryOnUnavailable = true))
        .leftMap(SequencerConnectionXInternalError.ClientError.apply)
      _ <- EitherT.cond[FutureUnlessShutdown](
        apiName == CantonGrpcUtil.ApiName.SequencerPublicApi,
        (),
        SequencerConnectionXInternalError.ValidationError(s"Bad API: $apiName"),
      )

      handshakeResponse <- client
        .performHandshake(
          clientProtocolVersions,
          minimumProtocolVersion,
          retryPolicy = retryPolicy(retryOnUnavailable = true),
        )
        .leftMap(SequencerConnectionXInternalError.ClientError.apply)
      handshakePV <- EitherT
        .fromEither[FutureUnlessShutdown](handshakeResponse match {
          case success: HandshakeResponse.Success =>
            Right(success.serverProtocolVersion)
          case _ => Left(SequencerConnectionXInternalError.ValidationError(s"Failed handshake"))
        })
      _ = logger.debug(s"Handshake successful with PV $handshakePV")

      // TODO(#23902): Might be good to have the crypto handshake part of connection validation as well

      info <- client
        .getSynchronizerAndSequencerIds(retryPolicy = retryPolicy(retryOnUnavailable = true))
        .leftMap[SequencerConnectionXInternalError](
          SequencerConnectionXInternalError.ClientError.apply
        )
      params <- client
        .getStaticSynchronizerParameters(retryPolicy = retryPolicy(retryOnUnavailable = true))
        .leftMap[SequencerConnectionXInternalError](
          SequencerConnectionXInternalError.ClientError.apply
        )
    } yield {
      val (synchronizerId, sequencerId) = info
      ConnectionAttributes(synchronizerId, sequencerId, params)
    }

    resultET.fold(handleFailedValidation, handleSuccessfulValidation)
  }

  override def onClosed(): Unit = LifeCycle.close(validationLimiter, connection)(logger)

  override protected def pretty: Pretty[GrpcSequencerConnectionX] =
    prettyOfClass(
      param("name", _.name.singleQuoted),
      param("attributes", _.attributesCell.get),
    )
}

object GrpcSequencerConnectionX {
  abstract class GrpcSequencerConnectionXHealth(
      override val name: String,
      override val associatedHasRunOnClosing: HasRunOnClosing,
      protected override val logger: TracedLogger,
  ) extends SequencerConnectionXHealth(name, associatedHasRunOnClosing, logger) {
    private[GrpcSequencerConnectionX] def refresh()(implicit traceContext: TraceContext): Unit
  }

  private sealed trait LocalState extends Product with Serializable with PrettyPrinting
  private sealed trait NonFatalLocalState extends LocalState

  /** {{{
    *                              fail()
    *      ┌────────────────────────────────────────────────┐
    *      │                                                │
    *      │         fail()                                 │
    *      │   ┌────────────────────┐                       │
    *      │   │                    │                       │
    *      │   │                    │                       │
    *  ┌───▼───▼──┐network     ┌────┴─────┐            ┌────┴────┐
    *  │ STOPPING │error during│ STARTING │            │VALIDATED│
    *  ├──────────┤validation  ├──────────┤ validation ├─────────┤
    *  │          ◄────────────┤on entry/ │ successful │         │
    *  │on entry/ │            │ start    ├────────────►         │
    *  │ stop     ├────────────►connection│            │         │
    *  │connection│  start()   │[started] ├─────┐      │         │
    *  │          │            │ request  │     │      │         │
    *  │          │            │validation│     │      │         │
    *  └──────────┘            └─────▲────┘     │      └─────────┘
    *                                │          │
    *                         start()│          │unrecoverable
    *                                │          │error during
    *                                │          │validation
    *                ┌─────────┐     │     ┌────▼────┐
    *                │ INITIAL │     │     │  FATAL  │
    *                ├─────────┤     │     ├─────────┤
    *                │         │     │     │         │ fatal()  from
    *                │         ├─────┘     │         ◄───────── anywhere
    *                │         │           │         │
    *                │         │           │         │
    *                └─────────┘           └─────────┘
    * }}}
    */
  private object LocalState {
    case object Fatal extends LocalState {
      override protected def pretty: Pretty[Fatal.type] = prettyOfObject[Fatal.type]
    }
    case object Initial extends NonFatalLocalState {
      override protected def pretty: Pretty[Initial.type] = prettyOfObject[Initial.type]
    }
    case object Starting extends NonFatalLocalState {
      override protected def pretty: Pretty[Starting.type] = prettyOfObject[Starting.type]
    }
    case object Stopping extends NonFatalLocalState {
      override protected def pretty: Pretty[Stopping.type] = prettyOfObject[Stopping.type]
    }
    case object Validated extends NonFatalLocalState {
      override protected def pretty: Pretty[Validated.type] = prettyOfObject[Validated.type]
    }
  }

  def create(
      config: ConnectionXConfig,
      clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
      minimumProtocolVersion: Option[ProtocolVersion],
      clientFactory: SequencerConnectionXClientFactory,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextExecutor
  ): SequencerConnectionX =
    new GrpcSequencerConnectionX(
      config,
      clientProtocolVersions,
      minimumProtocolVersion,
      clientFactory,
      futureSupervisor,
      timeouts,
      loggerFactory.append("connection", config.name),
    )
}
