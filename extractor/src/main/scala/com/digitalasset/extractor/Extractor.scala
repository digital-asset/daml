// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import akka.actor.ActorSystem
import akka.stream.scaladsl.{RestartSource, Sink}
import akka.stream.{KillSwitches, Materializer, RestartSettings}
import com.daml.auth.TokenHolder
import com.daml.extractor.Types._
import com.daml.extractor.config.{ExtractorConfig, SnapshotEndSetting}
import com.daml.extractor.helpers.FutureUtil.toFuture
import com.daml.extractor.helpers.TemplateIds
import com.daml.extractor.ledger.types.TransactionTree
import com.daml.extractor.ledger.types.TransactionTree._
import com.daml.extractor.writers.Writer
import com.daml.extractor.writers.Writer.RefreshPackages
import com.daml.grpc.GrpcException
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.api.{v1 => api}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration._
import com.daml.ledger.client.services.pkg.PackageClient
import com.daml.ledger.service.LedgerReader
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.timer.RetryStrategy
import com.typesafe.scalalogging.StrictLogging
import scalaz.\/
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.std.string._
import scalaz.syntax.apply._
import scalaz.syntax.foldable._
import scalaz.syntax.tag._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

class Extractor[T](config: ExtractorConfig, target: T)(
    writerSupplier: (ExtractorConfig, T, String) => Writer = Writer.apply _
) extends StrictLogging {

  private val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))
  private val parties: Set[String] = config.parties.toSet.map(identity)

  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher
  implicit val materializer: Materializer = Materializer(system)
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

      writer = writerSupplier(config, target, client.ledgerId.unwrap)

      _ = logger.info(s"Connected to ledger ${client.ledgerId}\n\n")

      endResponse <- client.transactionClient.getLedgerEnd(tokenHolder.flatMap(_.token))

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
        TemplateIds.intersection(allTemplateIds, config.templateConfigs)
      )

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
    } recoverWith { case NonFatal(fail) =>
      logger.error(s"FAILURE:\n$fail.\nExiting...")
      shutdown() *> Future.failed(fail)
    }
  }

  def shutdown(): Future[Unit] = {
    system.terminate().map(_ => killSwitch.shutdown())
  }

  private def keepRetryingOnPermissionDenied[A](f: () => Future[A]): Future[A] =
    RetryStrategy.constant(waitTime = 1.second) { case GrpcException.PERMISSION_DENIED() =>
      true
    } { (attempt, wait) =>
      logger.error(s"Failed to authenticate with Ledger API on attempt $attempt, next one in $wait")
      tokenHolder.foreach(_.refresh())
      f()
    }

  private def doFetchPackages(
      packageClient: PackageClient
  ): Future[LedgerReader.Error \/ Option[PackageStore]] =
    keepRetryingOnPermissionDenied { () =>
      LedgerReader.loadPackageStoreUpdates(packageClient, tokenHolder.flatMap(_.token))(Set.empty)
    }

  private def fetchPackages(client: LedgerClient, writer: Writer): Future[PackageStore] = {
    for {
      packageStoreE <- doFetchPackages(client.packageClient)
        .map(_.map(_.getOrElse(Map.empty))): Future[LedgerReader.Error \/ PackageStore]
      packageStore <- toFuture(packageStoreE): Future[PackageStore]
      _ <- writer.handlePackages(packageStore)
    } yield packageStore
  }

  private def selectTransactions(parties: ExtractorConfig.Parties): TransactionFilter = {
    // Template filtration is not supported on GetTransactionTrees RPC
    // we will have to filter out templates on the client-side.
    val templateSelection = Filters.defaultInstance
    TransactionFilter(parties.toList.view.map(_ -> templateSelection).toMap)
  }

  private def streamTransactions(
      client: LedgerClient,
      writer: Writer,
      streamUntil: Option[LedgerOffset],
      requestedTemplateIds: Set[api.value.Identifier],
  ): Future[Unit] = {
    logger.info(s"Requested template IDs: ${requestedTemplateIds}")

    val transactionFilter = selectTransactions(config.parties)
    logger.info(s"Setting transaction filter: ${transactionFilter}")

    RestartSource
      .onFailuresWithBackoff(
        RestartSettings(
          minBackoff = 3.seconds,
          maxBackoff = 30.seconds,
          randomFactor = 0.2, // adds 20% "noise" to vary the intervals slightly
        )
      ) { () =>
        tokenHolder.foreach(_.refresh())
        logger.info(s"Starting streaming transactions from ${startOffSet}...")
        client.transactionClient
          .getTransactionTrees(
            LedgerOffset(startOffSet),
            streamUntil,
            transactionFilter,
            verbose = true,
            tokenHolder.flatMap(_.token),
          )
          .via(killSwitch.flow)
          .collect {
            Function.unlift(convertTransactionTree(parties, requestedTemplateIds))
          }
          .mapAsync(parallelism = 1) { t =>
            writer
              .handleTransaction(t)
              .flatMap(
                _.fold(
                  handleUnwitnessedType(t, client, writer),
                  _ => transactionHandled(t),
                )
              )
          }
      }
      .via(killSwitch.flow)
      .runWith(Sink.ignore)
      .void
  }

  private def convertTransactionTree(parties: Set[String], templateIds: Set[Identifier])(
      t: api.transaction.TransactionTree
  ): Option[TransactionTree] = {
    val tree = t.convert(parties, templateIds).fold(e => throw DataIntegrityError(e), identity)
    if (tree.events.nonEmpty) {
      Some(tree)
    } else {
      None
    }
  }

  /** We encountered a transaction that reference a previously not witnessed type.
    * This is normal. Try re-fetch the packages first without reporting any errors.
    */
  private def handleUnwitnessedType(
      t: TransactionTree,
      client: LedgerClient,
      writer: Writer,
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
        _ => transactionHandled(t),
      )
    } yield ()
  }

  private def transactionHandled(t: TransactionTree): Future[Unit] = {
    startOffSet = LedgerOffset.Value.Absolute(t.offset)
    Future.unit
  }

  private def createClient: Future[LedgerClient] =
    LedgerClient.singleHost(
      config.ledgerHost,
      config.ledgerPort.value,
      LedgerClientConfiguration(
        applicationId = config.appId,
        ledgerIdRequirement = LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration(
          maxCommandsInFlight = 1,
          maxParallelSubmissions = 1,
          defaultDeduplicationTime = java.time.Duration.ofSeconds(20L),
        ),
        sslContext = config.tlsConfig.client(),
        token = tokenHolder.flatMap(_.token),
        maxInboundMessageSize = config.ledgerInboundMessageSizeMax,
      ),
    )

}
