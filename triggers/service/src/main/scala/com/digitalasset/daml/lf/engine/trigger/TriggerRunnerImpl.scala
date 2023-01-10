// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop, PreRestart}
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.daml.auth.middleware.api.Tagged.{AccessToken, RefreshToken}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.CompiledPackages
import com.daml.lf.engine.trigger.Runner.TriggerContext
import com.daml.lf.engine.trigger.ToLoggingContext._
import com.daml.lf.engine.trigger.TriggerRunner.{QueryingACS, Running, TriggerStatus}
import com.daml.logging.ContextualizedLogger
import io.grpc.Status.Code
import scalaz.syntax.tag._

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object TriggerRunnerImpl {

  final case class Config(
      server: ActorRef[Server.Message],
      triggerInstance: UUID,
      party: Party,
      applicationId: ApplicationId,
      accessToken: Option[AccessToken],
      refreshToken: Option[RefreshToken],
      compiledPackages: CompiledPackages,
      trigger: Trigger,
      triggerConfig: TriggerRunnerConfig,
      ledgerConfig: LedgerConfig,
      restartConfig: TriggerRestartConfig,
      readAs: Set[Party],
  ) {
    private[trigger] def withTriggerLogContext[T]: (TriggerLogContext => T) => T =
      Trigger.newTriggerLogContext(
        trigger.defn.id,
        party,
        readAs,
        triggerInstance.toString,
        applicationId,
      )
  }

  sealed trait Message
  final private case class Failed(error: Throwable) extends Message
  final private case class QueryACSFailed(cause: Throwable) extends Message
  final private case class QueriedACS(runner: Runner, acs: Seq[CreatedEvent], offset: LedgerOffset)
      extends Message
  final case class Status(replyTo: ActorRef[TriggerStatus]) extends Message

  private[this] val logger = ContextualizedLogger get getClass

  def apply(config: Config)(implicit
      esf: ExecutionSequencerFactory,
      mat: Materializer,
      triggerContext: TriggerLogContext,
  ): Behavior[Message] =
    Behaviors.setup { ctx =>
      val name = ctx.self.path.name
      implicit val ec: ExecutionContext = ctx.executionContext
      val triggerInstance = config.triggerInstance
      // Report to the server that this trigger is starting.
      config.server ! Server.TriggerStarting(triggerInstance)
      logger.info(s"Trigger $name is starting")
      val clientConfig = LedgerClientConfiguration(
        applicationId = config.applicationId.unwrap,
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default.copy(
          defaultDeduplicationTime = config.ledgerConfig.commandTtl
        ),
        token = AccessToken.unsubst(config.accessToken),
      )

      val channelConfig = LedgerClientChannelConfiguration(
        sslContext = None,
        maxInboundMessageSize = config.ledgerConfig.maxInboundMessageSize,
      )

      // Waiting for the ACS query to finish so we can build the
      // initial state.
      def queryingACS(): Behaviors.Receive[Message] =
        Behaviors.receiveMessagePartial[Message] {
          case Status(replyTo) =>
            replyTo ! QueryingACS
            Behaviors.same
          case QueryACSFailed(cause: io.grpc.StatusRuntimeException)
              if cause.getStatus.getCode == Code.UNAUTHENTICATED =>
            throw UnauthenticatedException(s"Querying ACS failed: ${cause.toString}")
          case QueryACSFailed(cause) =>
            // Report the failure to the server.
            config.server ! Server.TriggerInitializationFailure(triggerInstance, cause.toString)
            // Tell our monitor there's been a failure. The
            // monitor's supervision strategy will respond to this
            // (including logging the exception).
            throw new InitializationException("Couldn't start: " + cause.toString)
          case QueriedACS(runner, acs, offset) =>
            // It's possible for 'runWithACS' to throw (fail to
            // construct a flow).
            try {
              // The trigger is a future that we only expect to
              // complete if something goes wrong.
              val (killSwitch, trigger) = runner.runWithACS(
                acs,
                offset,
                msgFlow = KillSwitches.single[TriggerContext[TriggerMsg]],
              )

              // If we are stopped we will end up causing the future
              // to complete which will trigger a message that is
              // sent to a now terminated actor. In
              // https://doc.akka.io/docs/akka/current/general/message-delivery-reliability.html#dead-letters
              // it is explained that this is a somewhat ordinary
              // circumstance and not to be worried about.
              ctx.pipeToSelf(trigger) {
                case Success(_) =>
                  Failed(new RuntimeException("Trigger exited unexpectedly"))
                case Failure(cause) =>
                  Failed(cause)
              }
              // Report to the server that this trigger is entering
              // the running state.
              config.server ! Server.TriggerStarted(triggerInstance)
              logger.info(s"Trigger $name is starting")
              running(killSwitch)
            } catch {
              case NonFatal(cause) =>
                // Report the failure to the server.
                config.server ! Server.TriggerInitializationFailure(triggerInstance, cause.toString)
                logger.error(s"Trigger $name failed during initialization", cause)
                // Tell our monitor there's been a failure. The
                // monitor's supervisor strategy will respond to
                // this by writing the exception to the log and
                // attempting to restart this actor.
                throw new InitializationException("Couldn't start: " + cause.toString)
            }
        }

      // The trigger loop is running. The only thing to do now is wait
      // to be told to stop or respond to failures.
      def running(killSwitch: KillSwitch) =
        Behaviors
          .receiveMessagePartial[Message] {
            case Status(replyTo) =>
              replyTo ! Running
              Behaviors.same
            case Failed(cause: io.grpc.StatusRuntimeException)
                if cause.getStatus.getCode == Code.UNAUTHENTICATED =>
              throw UnauthenticatedException(s"Querying ACS failed: ${cause.toString}")
            case Failed(cause) =>
              // Report the failure to the server.
              config.server ! Server.TriggerRuntimeFailure(triggerInstance, cause.toString)
              logger.error(s"Trigger $name failed", cause)
              // Tell our monitor there's been a failure. The
              // monitor's supervisor strategy will respond to this by
              // writing the exception to the log and attempting to
              // restart this actor.
              throw new RuntimeException(cause)
          }
          .receiveSignal {
            case (_, PostStop) =>
              // Don't think about trying to send the server a message
              // here. It won't receive it (many Bothans died to bring
              // us this information).
              logger.info(s"Trigger $name stopped")
              killSwitch.shutdown()
              Behaviors.stopped
            case (_, PreRestart) =>
              // No need to send any messages here. The server has
              // already been informed of the earlier failure and in
              // the process of being restarted, will be informed of
              // the start along the way.
              logger.info(s"Trigger $name is being restarted")
              Behaviors.same
          }

      val acsQuery: Future[QueriedACS] = for {
        client <- LedgerClient.singleHost(
          config.ledgerConfig.host,
          config.ledgerConfig.port,
          clientConfig,
          channelConfig,
        )
        runner = Runner(
          config.compiledPackages,
          config.trigger,
          config.triggerConfig,
          client,
          config.ledgerConfig.timeProvider,
          config.applicationId,
          TriggerParties(
            actAs = config.party,
            readAs = config.readAs,
          ),
        )
        (acs, offset) <- runner.queryACS()
      } yield QueriedACS(runner, acs, offset)
      // Arrange for the completion status to be piped into a message
      // to this actor.
      ctx.pipeToSelf(acsQuery) {
        case Success(msg) => msg
        case Failure(cause) => QueryACSFailed(cause)
      }

      queryingACS()
    }
}
