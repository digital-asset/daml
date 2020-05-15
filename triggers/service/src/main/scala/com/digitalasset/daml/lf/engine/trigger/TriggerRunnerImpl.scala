// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.PostStop
import akka.actor.typed.PreRestart
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.daml.ledger.api.refinements.ApiTypes.Party
import io.grpc.netty.NettyChannelBuilder
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scalaz.syntax.tag._
import com.daml.lf.CompiledPackages
import com.daml.grpc.adapter.{ExecutionSequencerFactory}
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}

object TriggerRunnerImpl {
  case class Config(
      compiledPackages: CompiledPackages,
      trigger: Trigger,
      ledgerConfig: LedgerConfig,
      maxInboundMessageSize: Int,
      party: Party,
  )

  import TriggerRunner.{Message, Stop}
  final case class Failed(error: Throwable) extends Message
  final case class QueryACSFailed(cause: Throwable) extends Message
  final case class QueriedACS(runner: Runner, acs: Seq[CreatedEvent], offset: LedgerOffset)
      extends Message

  def apply(config: Config)(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): Behavior[Message] =
    Behaviors.setup { ctx =>
      implicit val ec: ExecutionContext = ctx.executionContext
      val name = ctx.self.path.name
      ctx.log.info(s"Trigger ${name} is starting")
      val appId = ApplicationId(name)
      val clientConfig = LedgerClientConfiguration(
        applicationId = appId.unwrap,
        ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
        commandClient = CommandClientConfiguration.default.copy(
          defaultDeduplicationTime = config.ledgerConfig.commandTtl),
        sslContext = None,
      )

      // Waiting for the ACS query to finish so we can build the
      // initial state.
      def queryingACS(wasStopped: Boolean): Behaviors.Receive[Message] =
        Behaviors.receiveMessagePartial[Message] {
          case QueryACSFailed(cause) =>
            if (wasStopped) {
              // Never mind that it failed - we were asked to stop
              // anyway.
              Behaviors.stopped;
            } else {
              throw new RuntimeException("ACS query failed", cause)
            }
          case QueriedACS(runner, acs, offset) =>
            if (wasStopped) {
              Behaviors.stopped;
            } else {
              val (killSwitch, trigger) = runner.runWithACS(
                acs,
                offset,
                msgFlow = KillSwitches.single[TriggerMsg],
                name,
              )
              // TODO If we are stopped we will end up causing the
              // future to complete which will trigger a message that
              // is sent to a now terminated actor. We should fix this
              // somehow™.
              ctx.pipeToSelf(trigger) {
                case Success(_) => Failed(new RuntimeException("Trigger exited unexpectedly"))
                case Failure(cause) => Failed(cause)
              }
              running(killSwitch)
            }
          case Stop =>
            // We got a stop message but the ACS query hasn't
            // completed yet.
            queryingACS(wasStopped = true)
        }

      // Trigger loop is started, wait until we should stop.
      def running(killSwitch: KillSwitch) =
        Behaviors
          .receiveMessagePartial[Message] {
            case Stop =>
              Behaviors.stopped
            case Failed(cause) =>
              // In the event 'runWithACS' completes it's because the
              // stream is broken. Throw an exception allowing our
              // supervisor to restart us.
              throw new RuntimeException(cause)
          }
          .receiveSignal {
            case (_, PostStop) =>
              ctx.log.info(s"Trigger ${name} is stopping")
              killSwitch.shutdown
              Behaviors.stopped
            case (_, PreRestart) =>
              ctx.log.info(s"Trigger ${name} is being restarted")
              Behaviors.same
          }

      val acsQuery: Future[QueriedACS] = for {
        client <- LedgerClient
          .fromBuilder(
            NettyChannelBuilder
              .forAddress(config.ledgerConfig.host, config.ledgerConfig.port)
              .maxInboundMessageSize(config.maxInboundMessageSize),
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

      ctx.pipeToSelf(acsQuery) {
        case Success(msg) => msg
        case Failure(cause) => QueryACSFailed(cause)
      }
      queryingACS(wasStopped = false)
    }
}
