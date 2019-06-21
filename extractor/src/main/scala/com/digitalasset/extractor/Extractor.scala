// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import akka.actor.ActorSystem
import akka.stream.{KillSwitches, ActorMaterializer}
import akka.stream.scaladsl.{RestartSource, Sink}
import com.digitalasset.extractor.Types._
import com.digitalasset.extractor.config.{ExtractorConfig, SnapshotEndSetting}
import com.digitalasset.ledger.service.LedgerReader
import com.digitalasset.extractor.ledger.types.TransactionTree
import com.digitalasset.extractor.ledger.types.TransactionTree._
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.extractor.targets.Target
import com.digitalasset.extractor.writers.Writer
import com.digitalasset.extractor.writers.Writer.RefreshPackages
import com.digitalasset.extractor.helpers.TemplateIds
import com.digitalasset.extractor.helpers.TransactionTreeTrimmer
import com.digitalasset.extractor.helpers.FutureUtil.toFuture
import com.digitalasset.grpc.adapter.{ExecutionSequencerFactory, AkkaExecutionSequencerPool}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.{v1 => api}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration._
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}

import scala.collection.breakOut
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.control.NonFatal
import scalaz._
import Scalaz._
import com.typesafe.scalalogging.StrictLogging
import scalaz.syntax.tag._

class Extractor[T <: Target](config: ExtractorConfig, target: T) extends StrictLogging {

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val esf: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool("esf-" + this.getClass.getSimpleName)(system)

  val killSwitch = KillSwitches.shared("transaction-stream")

  sys.addShutdownHook(killSwitch.shutdown())

  // Mutated when streaming transactions,
  // and by setting it after initializing the Writer (in case it's not a fresh run)
  @volatile var startOffSet: LedgerOffset.Value =
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)

  def run(): Future[Unit] = {
    val result = for {
      client <- createClient

      writer = Writer(config, target, client.ledgerId.unwrap)

      _ = logger.info(s"Connected to ledger ${client.ledgerId}\n\n")

      endResponse <- client.transactionClient.getLedgerEnd

      endOffset = endResponse.offset.getOrElse(
        throw new RuntimeException("Failed to get ledger end: response did not contain an offset.")
      )

      _ = logger.trace(s"Ledger end: ${endResponse}")

      _ = startOffSet = config.from.value

      _ <- writer.init()

      lastOffset <- writer.getLastOffset

      _ = lastOffset.foreach(l => startOffSet = LedgerOffset.Value.Absolute(l))

      _ = logger.trace("Handling packages...")

      packageStore <- fetchPackages(client, writer)
      allTemplateIds = TemplateIds.getTemplateIds(packageStore.values.toSet)
      _ = logger.info(s"All available template ids: ${allTemplateIds}")

      requestedTemplateIds <- Future.successful(
        TemplateIds.intersection(allTemplateIds, config.templateConfigs))

      streamUntil: Option[LedgerOffset] = config.to match {
        case SnapshotEndSetting.Head => Some(endOffset)
        case SnapshotEndSetting.Follow => None
        case SnapshotEndSetting.Until(offset) =>
          Some(LedgerOffset(LedgerOffset.Value.Absolute(offset)))
      }

      _ = logger.info("Handling transactions...")

      _ <- streamTransactions(client, writer, streamUntil, requestedTemplateIds)

      _ = logger.info("Done...")
    } yield ()

    result flatMap { _ =>
      logger.info("Success: extraction finished, exiting...")
      shutdown()
    } recoverWith {
      case NonFatal(fail) =>
        logger.error(s"FAILURE:\n$fail.\nExiting...")
        shutdown *> Future.failed(fail)
    }
  }

  def shutdown(): Future[Unit] = {
    system.terminate().map(_ => killSwitch.shutdown())
  }

  private def fetchPackages(client: LedgerClient, writer: Writer): Future[PackageStore] = {
    for {
      packageStoreE <- LedgerReader.createPackageStore(client): Future[String \/ PackageStore]
      packageStore <- toFuture(packageStoreE): Future[PackageStore]
      _ <- writer.handlePackages(packageStore)
    } yield packageStore
  }

  private def selectTransactions(parties: ExtractorConfig.Parties): TransactionFilter = {
    // Template filtration is not supported on GetTransactionTrees RPC
    // we will have to filter out templates on the client-side.
    val templateSelection = Filters.defaultInstance
    TransactionFilter(parties.toList.map(_ -> templateSelection)(breakOut))
  }

  private def streamTransactions(
      client: LedgerClient,
      writer: Writer,
      streamUntil: Option[LedgerOffset],
      requestedTemplateIds: Set[api.value.Identifier]
  ): Future[Unit] = {
    logger.info(s"Requested template IDs: ${requestedTemplateIds}")

    val transactionFilter = selectTransactions(config.parties)
    logger.info(s"Setting transaction filter: ${transactionFilter}")

    val trim: api.transaction.TransactionTree => api.transaction.TransactionTree =
      if (requestedTemplateIds.isEmpty) identity
      else TransactionTreeTrimmer.trim(requestedTemplateIds)

    RestartSource
      .onFailuresWithBackoff(
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
      ) { () =>
        logger.info(s"Starting streaming transactions from ${startOffSet}...")
        client.transactionClient
          .getTransactionTrees(
            LedgerOffset(startOffSet),
            streamUntil,
            transactionFilter,
            verbose = true
          )
          .via(killSwitch.flow)
          .map(trim)
          .collect {
            case t if nonEmpty(t) => convertTransactionTree(t)
          }
          .mapAsync(parallelism = 1) { t =>
            writer
              .handleTransaction(t)
              .flatMap(
                _.fold(
                  handleUnwitnessedType(t, client, writer),
                  _ => transactionHandled(t)
                )
              )
          }
      }
      .via(killSwitch.flow)
      .runWith(Sink.ignore)
      .void
  }

  private def nonEmpty(t: api.transaction.TransactionTree): Boolean = t.eventsById.nonEmpty

  private def convertTransactionTree(t: api.transaction.TransactionTree): TransactionTree =
    t.convert.fold(e => throw DataIntegrityError(e), identity)

  /**
    * We encountered a transaction that reference a previously not witnessed type.
    * This is normal. Try re-fetch the packages first without reporting any errors.
    */
  private def handleUnwitnessedType(
      t: TransactionTree,
      client: LedgerClient,
      writer: Writer
  )(
      c: RefreshPackages
  ): Future[Unit] = {
    logger.info(
      s"Encountered a previously unwitnessed type: ${c.missing}." +
        s" Refreshing packages..."
    )
    for {
      _ <- fetchPackages(client, writer)
      result <- writer.handleTransaction(t)
      _ <- result.fold(
        // We still don't have all the types available for the transaction.
        // One could wonder that what if this error was caused by another missing type?
        // That can't be the case, as we're processing the same transaction, so the
        // packages fetched _after_ we witnessed the said transaction _must_ contain
        // all type information.
        _ =>
          Future.failed(
            DataIntegrityError(s"Could not find information for type ${c.missing}")
        ),
        _ => transactionHandled(t)
      )
    } yield ()
  }

  private def transactionHandled(t: TransactionTree): Future[Unit] = {
    startOffSet = LedgerOffset.Value.Absolute(t.offset)
    Future.successful(())
  }

  private def createClient: Future[LedgerClient] = {
    val builder: NettyChannelBuilder = NettyChannelBuilder
      .forAddress(config.ledgerHost, config.ledgerPort)
      .maxInboundMessageSize(config.ledgerInboundMessageSizeMax)

    config.tlsConfig.client
      .fold {
        logger.debug(
          s"Connecting to ${config.ledgerHost}:${config.ledgerPort}, using a plaintext connection")
        builder.usePlaintext()
      } { sslContext =>
        logger.debug(s"Connecting to ${config.ledgerHost}:${config.ledgerPort}, using TLS")
        builder.sslContext(sslContext).negotiationType(NegotiationType.TLS)
      }

    val channel = builder.build()

    sys.addShutdownHook { channel.shutdownNow(); () }

    LedgerClient.forChannel(
      LedgerClientConfiguration(
        config.appId,
        LedgerIdRequirement(ledgerId = "", enabled = false),
        CommandClientConfiguration(1, 1, overrideTtl = true, java.time.Duration.ofSeconds(20L)),
        sslContext = config.tlsConfig.client
      ),
      channel
    )
  }
}
