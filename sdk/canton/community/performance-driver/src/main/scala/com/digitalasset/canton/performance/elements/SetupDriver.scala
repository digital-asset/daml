// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.{
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.retry.{
  AllExceptionRetryPolicy,
  ExceptionRetryPolicy,
  NoExceptionRetryPolicy,
  Success,
}
import com.digitalasset.canton.util.{MonadUtil, retry}
import com.google.protobuf.ByteString

import java.io.{FileInputStream, InputStream}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class SetupDriver(
    val loggerFactory: NamedLoggerFactory,
    val darPath: Option[String],
    synchronizers: Seq[SynchronizerId],
)(implicit
    ec: ExecutionContext,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends NamedLogging
    with FlagCloseable
    with NoTracing {

  override protected val timeouts: ProcessingTimeout = ProcessingTimeout()

  private def darFile(): ByteString = {

    // possible locations of the performance test dar
    val loadDarE: Either[String, InputStream] = darPath
      .map(path => Try(new FileInputStream(path)))
      .map(_.toEither.leftMap(x => s"Failed to load dar: ${x.getMessage}"))
      .getOrElse(
        Option(PerformanceRunner.getClass.getClassLoader.getResourceAsStream("PerformanceTest.dar"))
          .toRight("Can not load Performance.dar from resource path")
      )

    loadDarE match {
      case Right(is) => ByteString.readFrom(is)
      case Left(err) =>
        logger.error(err)
        throw new RuntimeException(err)
    }
  }

  def run(
      config: PerformanceRunnerConfig
  ): Future[Either[String, (LfPartyId, Map[String, LfPartyId])]] = {

    val clientConfig = LedgerClientConfiguration(
      userId = "Performance",
      commandClient = RateSettings.defaultCommandClientConfiguration,
    )

    val clientChannelConfig = LedgerClientChannelConfiguration(sslContext =
      config.ledger.tls.map(x => ClientChannelBuilder.sslContext(x))
    )

    val packageId = M.PackageID.id
    logger.info(s"Connecting to ${config.ledger}")

    val participantId = config.ledger.name
    for {
      client <- LedgerClient.singleHost(
        config.ledger.host,
        config.ledger.port.unwrap,
        clientConfig,
        clientChannelConfig,
        loggerFactory,
      )
      known <- client.packageManagementClient.listKnownPackages()
      _ <- {
        if (known.map(_.packageId).contains(packageId)) {
          logger.debug(s"Package of performance dar $packageId already exists")
          Future.unit
        } else {
          logger.debug("Uploading performance dar file")
          implicit val success: Success[Unit] = Success.always
          MonadUtil.sequentialTraverse_(synchronizers) { synchronizerId =>
            withRetry(NoExceptionRetryPolicy, operationName = "uploading performance dar file") {
              client.packageManagementClient.uploadDarFile(
                darFile(),
                synchronizerId = Some(synchronizerId.toProtoPrimitive),
              )
            }
          }
        }
      }
      relevantParties <- grabAllRelevantParties(client, config.master)
      mapped <- mapOrAddParties(
        x => allocateParty(client, participantId, config.synchronizerIds(x))(x),
        config.localRoles.map(_.name),
        relevantParties,
      )
      master <- relevantParties.find(_.startsWith(config.master)) match {
        case Some(master) => Future.successful(master)
        case None =>
          // Try a few times until the master party is observed.
          observeParty(
            client,
            participantId,
            config.master,
            config.synchronizerIds(config.master),
          )
      }
    } yield {
      logger.info(s"Completed setup phase by observing master party $master")
      Right((master, mapped))
    }
  }

  private def grabAllRelevantParties(client: LedgerClient, master: String) = {
    val assembled = mutable.ListBuffer[LfPartyId]()
    logger.info("Grabbing all relevant parties")
    val count = 1000
    val counter = new AtomicInteger(0)
    def go(pageToken: String): Future[Seq[LfPartyId]] =
      client.partyManagementClient
        .listKnownParties(pageToken = pageToken, pageSize = count)
        .flatMap { case (found, nextPage) =>
          val filtered = found
            .filter(d =>
              (d.isLocal || d.party.startsWith(master)) && !d.party.contains("bystander")
            )
            .map(_.party)
          val _ = assembled.appendAll(filtered)
          if (found.sizeIs == count) {
            logger.info("Grabbing batch at offset " + (counter.getAndIncrement() * count))
            go(nextPage)
          } else {
            logger.info(s"Finished finding with ${found.size}, with filtered=${assembled.size}")
            Future.successful(assembled.toSeq)
          }
        }
    go("")
  }

  private def observeParty(
      client: LedgerClient,
      participantId: String,
      party: String,
      configuredSynchronizers: Set[SynchronizerId],
  ): Future[LfPartyId] = {
    def retryUntilDefined[A](opName: String)(task: => Future[Option[A]]) = {
      implicit val success: Success[Option[A]] = retry.Success[Option[A]](x => x.isDefined)
      def getSucceeded(x: Option[A]): A = x.getOrElse(throw new RuntimeException(s"Failed $opName"))
      withRetry(AllExceptionRetryPolicy, opName)(task).map(getSucceeded)
    }

    retryUntilDefined(s"waiting for party $party to appear on $participantId") {
      client.partyManagementClient
        .listKnownParties()
        .map { case (parties, _) => parties.find(p => p.party.startsWith(party)) }
    }.flatMap { p =>
      retryUntilDefined(
        s"waiting for ${p.party} to be connected to $configuredSynchronizers synchronizers on $participantId"
      ) {
        client.stateService
          .getConnectedSynchronizers(p.party)
          .map(r =>
            if (
              configuredSynchronizers
                .map(_.toProtoPrimitive)
                .subsetOf(r.connectedSynchronizers.map(_.synchronizerId).toSet)
            ) Some(p.party)
            else None
          )
      }
    }
  }

  private def withRetry[Result](
      retryable: ExceptionRetryPolicy,
      operationName: String,
  )(task: => Future[Result])(implicit
      success: Success[Result]
  ): Future[Result] =
    retry.Pause(
      logger,
      this,
      maxRetries = 40,
      delay = 500.millis,
      operationName = operationName,
    )(task, retryable)

  private def allocateParty(
      client: LedgerClient,
      participantId: String,
      configuredSynchronizer: Set[SynchronizerId],
  )(party: String)(implicit ec: ExecutionContext): Future[LfPartyId] =
    for {
      lfParties <- MonadUtil.sequentialTraverse(configuredSynchronizer.toSeq)(synchronizer =>
        client.partyManagementClient
          .allocateParty(
            hint = Some(party),
            synchronizerId = Some(synchronizer.toProtoPrimitive),
          )
          .map { x =>
            logger.debug(
              s"Allocated new party ${x.party} for $party on $participantId on synchronizer $synchronizer"
            )
            x.party
          }
      )
      lfParty = lfParties.headOption.getOrElse(
        throw new IllegalArgumentException(
          "must specify at least 1 synchronizer to allocate a party"
        )
      )
      _ <- observeParty(client, participantId, lfParty, configuredSynchronizer)
    } yield lfParty

  private def mapOrAddParties(
      mkParty: String => Future[LfPartyId],
      expected: Set[String],
      localParties: Seq[LfPartyId],
  )(implicit ec: ExecutionContext): Future[Map[String, LfPartyId]] =
    Future
      .sequence(
        expected.toSeq.map(party =>
          localParties
            .find(x => x.startsWith(party)) // if existing party starts with this name
            .map { x =>
              logger.debug(s"Mapping party $party to existing $x")
              Future.successful(x)
            }
            .getOrElse(mkParty(party)) // otherwise create new
            .map(x => (party, x))
        )
      )
      .map(_.toMap)

}
