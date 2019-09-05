// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeUnit

import com.daml.ledger.api.testtool.infrastructure.LedgerServices
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndRequest
import io.grpc.ManagedChannel
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

private[participant] object ParticipantSession {

  private[this] val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  private[this] val timer: Timer = new Timer("create-ledger-context-retry-scheduler", true)

  private def after[T](duration: FiniteDuration)(value: => Future[T])(
      implicit ec: ExecutionContext): Future[T] =
    if (duration.isFinite && duration.length < 1) {
      try value
      catch { case NonFatal(t) => Future.failed(t) }
    } else {
      val p = Promise[T]()
      timer.schedule(new TimerTask {
        override def run(): Unit = {
          p.completeWith {
            try value
            catch { case NonFatal(t) => Future.failed(t) }
          }
        }
      }, duration.toMillis)
      p.future
    }

  private def withBackoff(
      maxAttempts: Int,
      minWait: FiniteDuration,
      maxWait: FiniteDuration,
      backoffProgression: FiniteDuration => FiniteDuration)(
      poll: () => Future[ParticipantTestContext])(
      implicit ec: ExecutionContext): Future[ParticipantTestContext] = {
    def go(attempt: Int, waitTime: FiniteDuration): Future[ParticipantTestContext] = {
      logger.debug(s"Creating ledger context (attempt #$attempt)...")
      poll()
        .recoverWith {
          case throwable if attempt > maxAttempts =>
            Future.failed(throwable)
          case _ =>
            after(waitTime)(
              go(attempt + 1, backoffProgression(waitTime).min(maxWait).max(50.milliseconds)))
        }
    }

    go(1, minWait.min(maxWait).max(50.milliseconds))
  }

}

private[participant] final class ParticipantSession(
    val config: ParticipantSessionConfiguration,
    channel: ManagedChannel,
    eventLoopGroup: NioEventLoopGroup)(implicit val executionContext: ExecutionContext) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  private[this] val services: LedgerServices = new LedgerServices(channel)

  private[testtool] def createTestContext(
      endpointId: String,
      applicationId: String,
      identifierSuffix: String): Future[ParticipantTestContext] =
    ParticipantSession.withBackoff(10, 50.milliseconds, 10.seconds, _ * 2) { () =>
      for {
        id <- services.identity.getLedgerIdentity(new GetLedgerIdentityRequest).map(_.ledgerId)
        end <- services.transaction.getLedgerEnd(new GetLedgerEndRequest(id)).map(_.getOffset)
      } yield
        new ParticipantTestContext(
          id,
          endpointId,
          applicationId,
          identifierSuffix,
          end,
          services,
          config.commandTtlFactor)
    }

  private[testtool] def close(): Unit = {
    logger.info(s"Disconnecting from participant at ${config.host}:${config.port}...")
    channel.shutdownNow()
    if (!channel.awaitTermination(10L, TimeUnit.SECONDS)) {
      sys.error("Channel shutdown stuck. Unable to recover. Terminating.")
    }
    logger.info(s"Connection to participant at ${config.host}:${config.port} shut down.")
    if (!eventLoopGroup
        .shutdownGracefully(0, 0, TimeUnit.SECONDS)
        .await(10L, TimeUnit.SECONDS)) {
      sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
    }
    logger.info(s"Connection to participant at ${config.host}:${config.port} closed.")
  }

}
