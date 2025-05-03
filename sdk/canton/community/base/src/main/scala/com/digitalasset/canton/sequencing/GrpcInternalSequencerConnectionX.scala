// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{AtomicHealthElement, CompositeHealthElement, HealthListener}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasRunOnClosing, LifeCycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.sequencing.ConnectionX.{ConnectionXConfig, ConnectionXState}
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXError,
  SequencerConnectionXHealth,
  SequencerConnectionXInternalError,
  SequencerConnectionXState,
}
import com.digitalasset.canton.sequencing.SequencerConnectionXStub.SequencerConnectionXStubError
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.transports.{
  GrpcClientTransportHelpers,
  GrpcSequencerClientAuth,
}
import com.digitalasset.canton.sequencing.protocol.HandshakeResponse
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, blocking}
import scala.util.Failure

/** Sequencer connection specialized for gRPC transport.
  */
class GrpcInternalSequencerConnectionX private[sequencing] (
    override val config: ConnectionXConfig,
    clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
    minimumProtocolVersion: Option[ProtocolVersion],
    stubFactory: SequencerConnectionXStubFactory,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends InternalSequencerConnectionX
    with PrettyPrinting
    with GrpcClientTransportHelpers {
  import GrpcInternalSequencerConnectionX.*

  private val connection: GrpcConnectionX = GrpcConnectionX(config, timeouts, loggerFactory)
  private val stub: SequencerConnectionXStub = stubFactory.createStub(connection)
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
        case (ConnectionXState.Stopped, LocalState.Initial) => SequencerConnectionXState.Initial
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
      override def name: String =
        s"GrpcInternalSequencerConnectionX-${GrpcInternalSequencerConnectionX.this.name}"

      override def poke()(implicit traceContext: TraceContext): Unit = {
        val state = health.getState
        logger.debug(s"Poked with state $state")

        state match {
          case SequencerConnectionXState.Started =>
            FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
              performUnlessClosingUSF(
                s"validate connection ${GrpcInternalSequencerConnectionX.this.name}"
              )(
                validationLimiter.maybeValidate().thereafter {
                  case Failure(t) =>
                    // If a validation throws, we consider this a fatal failure of the connection
                    fatal(reason = s"Validation failed with an exception: $t")
                  case _ =>
                }
              ),
              s"validate connection ${GrpcInternalSequencerConnectionX.this.name}",
            )

          case _ => // No action
        }
      }

    })
    .discard[Boolean]

  override def name: String = s"internal-sequencer-${connection.name}"

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

      case SequencerConnectionXInternalError.StubError(
            SequencerConnectionXStubError.DeserializationError(error)
          ) =>
        logger.info(s"Deserialization error: $error")
        fatal("Deserialization error")

      case SequencerConnectionXInternalError.StubError(
            SequencerConnectionXStubError.ConnectionError(error)
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
      apiName <- stub
        .getApiName(retryPolicy = retryPolicy(retryOnUnavailable = true))
        .leftMap(SequencerConnectionXInternalError.StubError.apply)
      _ <- EitherT.cond[FutureUnlessShutdown](
        apiName == CantonGrpcUtil.ApiName.SequencerPublicApi,
        (),
        SequencerConnectionXInternalError.ValidationError(s"Bad API: $apiName"),
      )

      handshakeResponse <- stub
        .performHandshake(
          clientProtocolVersions,
          minimumProtocolVersion,
          retryPolicy = retryPolicy(retryOnUnavailable = true),
        )
        .leftMap(SequencerConnectionXInternalError.StubError.apply)
      handshakePV <- EitherT
        .fromEither[FutureUnlessShutdown](handshakeResponse match {
          case success: HandshakeResponse.Success =>
            Right(success.serverProtocolVersion)
          case _ => Left(SequencerConnectionXInternalError.ValidationError(s"Failed handshake"))
        })
      _ = logger.debug(s"Handshake successful with PV $handshakePV")

      // TODO(#23902): Might be good to have the crypto handshake part of connection validation as well

      info <- stub
        .getSynchronizerAndSequencerIds(retryPolicy = retryPolicy(retryOnUnavailable = true))
        .leftMap[SequencerConnectionXInternalError](
          SequencerConnectionXInternalError.StubError.apply
        )
      params <- stub
        .getStaticSynchronizerParameters(retryPolicy = retryPolicy(retryOnUnavailable = true))
        .leftMap[SequencerConnectionXInternalError](
          SequencerConnectionXInternalError.StubError.apply
        )
    } yield {
      val (synchronizerId, sequencerId) = info
      ConnectionAttributes(synchronizerId, sequencerId, params)
    }

    resultET.fold(handleFailedValidation, handleSuccessfulValidation)
  }

  override def buildUserConnection(
      authConfig: AuthenticationTokenManagerConfig,
      member: Member,
      crypto: SynchronizerCrypto,
      clock: Clock,
  ): Either[SequencerConnectionXError, GrpcSequencerConnectionX] =
    connection.channel
      .toRight(
        SequencerConnectionXError.InvalidStateError("Channel is not available")
      )
      .map { channel =>
        val clientAuth = new GrpcSequencerClientAuth(
          synchronizerId = checked(tryAttributes).synchronizerId,
          member = member,
          crypto = crypto,
          channelPerEndpoint = NonEmpty(Map, config.endpoint -> channel.channel),
          supportedProtocolVersions = clientProtocolVersions,
          tokenManagerConfig = authConfig,
          clock = clock,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )

        val authenticatedStub = stubFactory.createUserStub(connection, clientAuth)
        new GrpcSequencerConnectionX(
          this,
          name = s"sequencer-${connection.name}",
          authenticatedStub,
          timeouts,
          loggerFactory,
        )
      }

  override def onClosed(): Unit = LifeCycle.close(validationLimiter, connection)(logger)

  override protected def pretty: Pretty[GrpcInternalSequencerConnectionX] =
    prettyOfClass(
      param("name", _.name.singleQuoted),
      param("attributes", _.attributesCell.get),
    )
}

object GrpcInternalSequencerConnectionX {
  abstract class GrpcSequencerConnectionXHealth(
      override val name: String,
      override val associatedHasRunOnClosing: HasRunOnClosing,
      protected override val logger: TracedLogger,
  ) extends SequencerConnectionXHealth(name, associatedHasRunOnClosing, logger) {
    private[GrpcInternalSequencerConnectionX] def refresh()(implicit
        traceContext: TraceContext
    ): Unit
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
}

class GrpcInternalSequencerConnectionXFactory(
    clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
    minimumProtocolVersion: Option[ProtocolVersion],
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends InternalSequencerConnectionXFactory {
  override def create(config: ConnectionXConfig)(implicit
      ec: ExecutionContextExecutor
  ): InternalSequencerConnectionX =
    new GrpcInternalSequencerConnectionX(
      config,
      clientProtocolVersions,
      minimumProtocolVersion,
      stubFactory = SequencerConnectionXStubFactoryImpl,
      futureSupervisor,
      timeouts,
      loggerFactory.append("connection", config.name),
    )
}
