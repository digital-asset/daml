// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, Behavior, PreRestart, PostStop}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.daml.ledger.api.refinements.ApiTypes.Party
import io.grpc.netty.NettyChannelBuilder

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz.syntax.tag._
import com.daml.lf.CompiledPackages
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import java.util.UUID

object TriggerRunnerImpl {
  case class Config(
      server: ActorRef[Message],
      triggerInstance: UUID,
      party: Party,
      // TODO(SF, 2020-06-09): Add access token field here in the presence of authentication.
      compiledPackages: CompiledPackages,
      trigger: Trigger,
      ledgerConfig: LedgerConfig,
      restartConfig: TriggerRestartConfig,
  )

  import TriggerRunner.{Message, Stop}
  final private case class Failed(error: Throwable) extends Message
  final private case class QueryACSFailed(cause: Throwable) extends Message
  final private case class QueriedACS(runner: Runner, acs: Seq[CreatedEvent], offset: LedgerOffset)
      extends Message

  def apply(config: Config)(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): Behavior[Message] =
    Behaviors.setup { ctx =>
      val name = ctx.self.path.name
      implicit val ec: ExecutionContext = ctx.executionContext
      val triggerInstance = config.triggerInstance
      // Report to the server that this trigger is starting.
      config.server ! TriggerStarting(triggerInstance)
      ctx.log.info(s"Trigger $name is starting")
      val appId = ApplicationId(name)
      val clientConfig = LedgerClientConfiguration(
        applicationId = appId.unwrap,
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default.copy(
          defaultDeduplicationTime = config.ledgerConfig.commandTtl),
        sslContext = None,
        // TODO(SF, 2020-06-09): In the presence of an authorization
        // service, get an access token and pass it through here!
        token = None
      )

      // Waiting for the ACS query to finish so we can build the
      // initial state.
      def queryingACS(wasStopped: Boolean): Behaviors.Receive[Message] =
        Behaviors.receiveMessagePartial[Message] {
          case QueryACSFailed(cause) =>
            if (wasStopped) {
              // The stop endpoint can't send a message to a runner
              // that isn't in the running triggers table so this is
              // an odd case.
              config.server ! TriggerInitializationFailure(triggerInstance, cause.toString)
              // However we got here though, one thing is clear. We
              // don't want to restart the actor.
              throw new InitializationHalted("User stopped") // Don't retry!
            } else {
              // Report the failure to the server.
              config.server ! TriggerInitializationFailure(triggerInstance, cause.toString)
              // Tell our monitor there's been a failure. The
              // monitor's supervision strategy will respond to this
              // (including logging the exception).
              throw new InitializationException("Couldn't start: " + cause.toString)
            }
          case QueriedACS(runner, acs, offset) =>
            if (wasStopped) {
              // The stop endpoint can't send a message to a runner
              // that isn't in the running triggers table so this is
              // an odd case.
              config.server ! TriggerInitializationFailure(triggerInstance, "User stopped")
              // However we got here though, one thing is clear. We
              // don't want to restart the actor.
              throw new InitializationHalted("User stopped") // Don't retry!
            } else {
              // It's possible for 'runWithACS' to throw (fail to
              // construct a flow).
              try {
                // The trigger is a future that we only expect to
                // complete if something goes wrong.
                val (killSwitch, trigger) = runner.runWithACS(
                  acs,
                  offset,
                  msgFlow = KillSwitches.single[TriggerMsg],
                  name,
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
                config.server ! TriggerStarted(triggerInstance)
                running(killSwitch)
              } catch {
                case cause: Throwable =>
                  // Report the failure to the server.
                  config.server ! TriggerInitializationFailure(triggerInstance, cause.toString)
                  // Tell our monitor there's been a failure. The
                  // monitor's supervisor strategy will respond to
                  // this by writing the exception to the log and
                  // attempting to restart this actor.
                  throw new InitializationException("Couldn't start: " + cause.toString)
              }
            }
          case Stop =>
            // We got a stop message but the ACS query hasn't
            // completed yet.
            queryingACS(wasStopped = true)
        }

      // The trigger loop is running. The only thing to do now is wait
      // to be told to stop or respond to failures.
      def running(killSwitch: KillSwitch) =
        Behaviors
          .receiveMessagePartial[Message] {
            case Stop =>
              // Don't think about trying to send the server a message
              // here. It won't receive it (I found out the hard way).
              Behaviors.stopped
            case Failed(cause) =>
              // Report the failure to the server.
              config.server ! TriggerRuntimeFailure(triggerInstance, cause.toString)
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
              ctx.log.info(s"Trigger $name is stopping")
              killSwitch.shutdown
              Behaviors.stopped
            case (_, PreRestart) =>
              // No need to send any messages here. The server has
              // already been informed of the earlier failure and in
              // the process of being restarted, will be informed of
              // the start along the way.
              ctx.log.info(s"Trigger $name is being restarted")
              Behaviors.same
          }

      val acsQuery: Future[QueriedACS] = for {
        client <- LedgerClient
          .fromBuilder(
            NettyChannelBuilder
              .forAddress(config.ledgerConfig.host, config.ledgerConfig.port)
              .maxInboundMessageSize(config.ledgerConfig.maxInboundMessageSize),
            clientConfig,
          )
        runner = new Runner(
          config.compiledPackages,
          config.trigger,
          client,
          config.ledgerConfig.timeProvider,
          appId,
          config.party.unwrap)
        (acs, offset) <- runner.queryACS()
      } yield QueriedACS(runner, acs, offset)
      // Arrange for the completion status to be piped into a message
      // to this actor.
      ctx.pipeToSelf(acsQuery) {
        case Success(msg) => msg
        case Failure(cause) => QueryACSFailed(cause)
      }

      queryingACS(wasStopped = false)
    }
}
