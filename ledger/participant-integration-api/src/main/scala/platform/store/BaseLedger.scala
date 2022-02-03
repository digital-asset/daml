// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.codahale.metrics.Timer
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.definitions.IndexErrors.IndexDbException
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{GetFlatTransactionResponse, GetTransactionResponse, GetTransactionTreesResponse, GetTransactionsResponse}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.{ContractStore, MaximumLedgerTime}
import com.daml.ledger.participant.state.index.v2.MeteringStore.TransactionMetering
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.ValueEnricher
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.index.BuffersUpdater
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events.{BufferedTransactionsReader, LfValueTranslation}
import com.daml.platform.store.appendonlydao.{JdbcLedgerDao, LedgerDaoTransactionsReader, LedgerReadDao}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.SignalNewLedgerHead
import com.daml.platform.store.cache.{EventsBuffer, LedgerEndCache, MutableCacheBackedContractStore, MutableLedgerEndCache}
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.{StringInterning, StringInterningView, UpdatingStringInterningView}
import com.daml.platform.{PruneBuffers, PruneBuffersNoOp}
import com.daml.resources.ProgramResource.StartupException
import com.daml.scalautil.Statement.discard
import com.daml.timer.RetryStrategy

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

private[platform] class BaseLedger(
    val ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    transactionsReader: LedgerDaoTransactionsReader,
    contractStore: ContractStore,
    pruneBuffers: PruneBuffers,
    contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
    dispatcher: Dispatcher[Offset],
    dispatcherLagger: DispatcherLagMeter,
    updatingStringInterningView: UpdatingStringInterningView,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends ReadOnlyLedger {

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupKey(key: GlobalKey, forParties: Set[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    contractStore.lookupContractKey(forParties, key)

  override def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.beforeBegin),
      RangeSource(transactionsReader.getFlatTransactions(_, _, filter, verbose)),
      endInclusive,
    )

  override def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Ref.Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.beforeBegin),
      RangeSource(
        transactionsReader.getTransactionTrees(_, _, requestingParties, verbose)
      ),
      endInclusive,
    )

  override def ledgerEnd()(implicit loggingContext: LoggingContext): Offset = dispatcher.getHead()

  override def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[(Offset, CompletionStreamResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.beforeBegin),
      RangeSource(ledgerDao.completions.getCommandCompletions(_, _, applicationId, parties)),
      endInclusive,
    )

  override def activeContracts(
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): (Source[GetActiveContractsResponse, NotUsed], Offset) = {
    val activeAt = ledgerEnd()
    (ledgerDao.transactionsReader.getActiveContracts(activeAt, filter, verbose), activeAt)
  }

  override def lookupContract(
      contractId: ContractId,
      forParties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[VersionedContractInstance]] =
    contractStore.lookupActiveContract(forParties, contractId)

  override def lookupFlatTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    ledgerDao.transactionsReader.lookupFlatTransactionById(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    ledgerDao.transactionsReader.lookupTransactionTreeById(transactionId, requestingParties)

  override def lookupMaximumLedgerTime(
      contractIds: Set[ContractId]
  )(implicit loggingContext: LoggingContext): Future[MaximumLedgerTime] =
    contractStore.lookupMaximumLedgerTimeAfterInterpretation(contractIds)

  override def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[domain.PartyDetails]] =
    ledgerDao.getParties(parties)

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[domain.PartyDetails]] =
    ledgerDao.listKnownParties()

  override def partyEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, PartyLedgerEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getPartyEntries))

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, v2.PackageDetails]] =
    ledgerDao.listLfPackages()

  override def getLfArchive(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[DamlLf.Archive]] =
    ledgerDao.getLfArchive(packageId)

  override def packageEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, PackageLedgerEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getPackageEntries))

  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    ledgerDao.lookupLedgerConfiguration()

  override def configurationEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, ConfigurationEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getConfigurationEntries))

  override def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    pruneBuffers(pruneUpToInclusive)
    ledgerDao.prune(pruneUpToInclusive, pruneAllDivulgedContracts)
  }

  override def getTransactionMetering(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[Ref.ApplicationId],
  )(implicit loggingContext: LoggingContext): Future[Vector[TransactionMetering]] = {
    ledgerDao.getTransactionMetering(from, to, applicationId)
  }

  private val (ledgerEndUpdateKillSwitch, ledgerEndUpdateDone) =
    RestartSource
      .withBackoff(
        RestartSettings(minBackoff = 1.second, maxBackoff = 10.seconds, randomFactor = 0.2)
      )(() =>
        Source
          .tick(0.millis, 100.millis, ())
          .mapAsync(1) {
            implicit val ec: ExecutionContext = mat.executionContext
            _ =>
              for {
                ledgerEnd <- ledgerDao.lookupLedgerEnd()
                _ <- updatingStringInterningView.update(ledgerEnd.lastStringInterningId)
              } yield ledgerEnd
          }
      )
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.foreach { newLedgerHead =>
        dispatcherLagger.startTimer(newLedgerHead.lastOffset)
        contractStateEventsDispatcher.signalNewHead(
          newLedgerHead.lastOffset -> newLedgerHead.lastEventSeqId
        )
      })(
        Keep.both[UniqueKillSwitch, Future[Done]]
      )
      .run()

  def close(): Unit = {
    ledgerEndUpdateKillSwitch.shutdown()
    Await.ready(ledgerEndUpdateDone, 10.seconds)
    ()
  }
}

private[platform] object BaseLedger {
  private val logger = ContextualizedLogger.get(this.getClass)

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  final class Owner(
      dbSupport: DbSupport,
      initialLedgerId: LedgerId,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      acsIdPageSize: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
      acsGlobalParallelism: Int,
      acsIdQueueLimit: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: ValueEnricher,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Long,
      enableInMemoryFanOutForLedgerApi: Boolean,
      participantId: Ref.ParticipantId,
      errorFactories: ErrorFactories,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[BaseLedger] {

    override def acquire()(implicit context: ResourceContext): Resource[BaseLedger] = {
      val ledgerEndCache = MutableLedgerEndCache()
      val stringInterningStorageBackend =
        dbSupport.storageBackendFactory.createStringInterningStorageBackend
      val stringInterningView = new StringInterningView(
        loadPrefixedEntries = (fromExclusive, toInclusive) =>
          implicit loggingContext =>
            dbSupport.dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
              stringInterningStorageBackend.loadStringInterningEntries(
                fromExclusive,
                toInclusive,
              )
            }
      )
      val ledgerDao = createLedgerReadDao(ledgerEndCache, stringInterningView)
      for {
        ledgerId <- Resource.fromFuture(verifyLedgerId(ledgerDao, initialLedgerId))
        ledgerEnd <- Resource.fromFuture(ledgerDao.lookupLedgerEnd())
        _ = ledgerEndCache.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId))
        _ <- Resource.fromFuture(stringInterningView.update(ledgerEnd.lastStringInterningId))
        prefetchingDispatcher <- dispatcherOffsetSeqIdOwner(
          ledgerEnd.lastOffset,
          ledgerEnd.lastEventSeqId,
        ).acquire()
        generalDispatcher <- dispatcherOwner(ledgerEnd.lastOffset).acquire()
        dispatcherLagMeter <- Resource.successful(
          new DispatcherLagMeter((offset, eventSeqId) => {
            ledgerEndCache.set((offset, eventSeqId))
            // the order here is very important: first we need to make data available for point-wise lookups
            // and SQL queries, and only then we can make it available on the streams.
            // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
            generalDispatcher.signalNewHead(offset)
          })(metrics.daml.execution.cache.dispatcherLag)
        )
        ledger <- ledgerOwner(
          ledgerDao,
          prefetchingDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
          ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId,
          ledgerId,
          stringInterningView,
        ).acquire()
      } yield ledger
    }

    private def ledgerOwner(
        ledgerReadDao: LedgerReadDao,
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
        startExclusive: (Offset, Long),
        ledgerId: LedgerId,
        stringInterningView: StringInterningView,
    ): ResourceOwner[BaseLedger] =
      if (enableInMemoryFanOutForLedgerApi)
        ledgerWithMutableCacheAndInMemoryFanOut(
          ledgerReadDao,
          cacheUpdatesDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
          startExclusive,
          ledgerId,
          stringInterningView,
        )
      else
        ledgerWithMutableCache(
          ledgerReadDao,
          cacheUpdatesDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
          startExclusive,
          ledgerId,
          stringInterningView,
        )

    private def ledgerWithMutableCache(
        ledgerReadDao: LedgerReadDao,
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
        startExclusive: (Offset, Long),
        ledgerId: LedgerId,
        stringInterningView: StringInterningView,
    ): ResourceOwner[BaseLedger] =
      for {
        contractStore <- new MutableCacheBackedContractStore.OwnerWithSubscription(
          subscribeToContractStateEvents = cacheIndex =>
            cacheUpdatesDispatcher
              .startingAt(
                cacheIndex,
                RangeSource(
                  ledgerReadDao.transactionsReader.getContractStateEvents(_, _)
                ),
              )
              .map(_._2),
          contractsReader = ledgerReadDao.contractsReader,
          signalNewLedgerHead = dispatcherLagMeter,
          startIndexExclusive = startExclusive,
          metrics = metrics,
          maxContractsCacheSize = maxContractStateCacheSize,
          maxKeyCacheSize = maxContractKeyStateCacheSize,
          executionContext = servicesExecutionContext,
        )
        ledger <- ResourceOwner.forCloseable(() =>
          new BaseLedger(
            ledgerId,
            ledgerReadDao,
            ledgerReadDao.transactionsReader,
            contractStore,
            PruneBuffersNoOp,
            cacheUpdatesDispatcher,
            generalDispatcher,
            dispatcherLagMeter,
            stringInterningView,
          )
        )
      } yield ledger

    private def ledgerWithMutableCacheAndInMemoryFanOut(
        ledgerReadDao: LedgerReadDao,
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
        startExclusive: (Offset, Long),
        ledgerId: LedgerId,
        stringInterningView: StringInterningView,
    ): ResourceOwner[BaseLedger] = {
      val transactionsBuffer = new EventsBuffer[Offset, TransactionLogUpdate](
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "transactions",
        isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
      )

      val contractStore = MutableCacheBackedContractStore(
        ledgerReadDao.contractsReader,
        dispatcherLagMeter,
        startExclusive,
        metrics,
        maxContractStateCacheSize,
        maxContractKeyStateCacheSize,
      )(servicesExecutionContext, loggingContext)

      val bufferedTransactionsReader = BufferedTransactionsReader(
        delegate = ledgerReadDao.transactionsReader,
        transactionsBuffer = transactionsBuffer,
        lfValueTranslation = new LfValueTranslation(
          cache = LfValueTranslationCache.Cache.none,
          metrics = metrics,
          enricherO = Some(enricher),
          loadPackage =
            (packageId, loggingContext) => ledgerReadDao.getLfArchive(packageId)(loggingContext),
        ),
        metrics = metrics,
      )(loggingContext, servicesExecutionContext)

      for {
        _ <- ResourceOwner.forCloseable(() =>
          BuffersUpdater(
            subscribeToTransactionLogUpdates = maybeOffsetSeqId => {
              val subscriptionStartExclusive @ (offsetStart, eventSeqIdStart) =
                maybeOffsetSeqId.getOrElse(startExclusive)
              logger.info(
                s"Subscribing for transaction log updates after ${offsetStart.toHexString} -> $eventSeqIdStart"
              )
              cacheUpdatesDispatcher
                .startingAt(
                  subscriptionStartExclusive,
                  RangeSource(
                    ledgerReadDao.transactionsReader.getTransactionLogUpdates(_, _)
                  ),
                )
            },
            updateTransactionsBuffer = transactionsBuffer.push,
            updateMutableCache = contractStore.push,
            executionContext = servicesExecutionContext,
          )
        )
        ledger <- ResourceOwner.forCloseable(() =>
          new BaseLedger(
            ledgerId = ledgerId,
            ledgerDao = ledgerReadDao,
            transactionsReader = bufferedTransactionsReader,
            pruneBuffers = transactionsBuffer.prune,
            contractStore = contractStore,
            contractStateEventsDispatcher = cacheUpdatesDispatcher,
            dispatcher = generalDispatcher,
            dispatcherLagger = dispatcherLagMeter,
            updatingStringInterningView = stringInterningView,
          )
        )
      } yield ledger
    }

    private def dispatcherOffsetSeqIdOwner(ledgerEnd: Offset, evtSeqId: Long) = {
      Dispatcher.owner(
        name = "transaction-log-updates",
        zeroIndex = (Offset.beforeBegin, EventSequentialId.beforeBegin),
        headAtInitialization = (ledgerEnd, evtSeqId),
      )
    }

    private def dispatcherOwner(ledgerEnd: Offset): ResourceOwner[Dispatcher[Offset]] =
      Dispatcher.owner(
        name = "sql-ledger",
        zeroIndex = Offset.beforeBegin,
        headAtInitialization = ledgerEnd,
      )

    private def verifyLedgerId(
        ledgerDao: LedgerReadDao,
        initialLedgerId: LedgerId,
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[LedgerId] = {
      // If the index database is not yet fully initialized,
      // querying for the ledger ID will throw different errors,
      // depending on the database, and how far the initialization is.
      val isRetryable: PartialFunction[Throwable, Boolean] = {
        case _: IndexDbException => true
        case _: LedgerIdNotFoundException => true
        case _: MismatchException.LedgerId => false
        case _ => false
      }
      val retryDelay = 100.millis
      val maxAttempts = 3000 // give up after 5min
      RetryStrategy.constant(attempts = Some(maxAttempts), waitTime = retryDelay)(isRetryable) {
        (attempt, _) =>
          ledgerDao
            .lookupLedgerId()
            .flatMap {
              case Some(`initialLedgerId`) =>
                logger.info(s"Found existing ledger with ID: $initialLedgerId")
                Future.successful(initialLedgerId)
              case Some(foundLedgerId) =>
                Future.failed(
                  new MismatchException.LedgerId(foundLedgerId, initialLedgerId)
                    with StartupException
                )
              case None =>
                logger.info(
                  s"Ledger ID not found in the index database on attempt $attempt/$maxAttempts. Retrying again in $retryDelay."
                )
                Future.failed(new LedgerIdNotFoundException(attempt))
            }
      }
    }

    private def createLedgerReadDao(
        ledgerEndCache: LedgerEndCache,
        stringInterning: StringInterning,
    ): LedgerReadDao =
      JdbcLedgerDao.read(
        dbSupport = dbSupport,
        eventsPageSize = eventsPageSize,
        eventsProcessingParallelism = eventsProcessingParallelism,
        acsIdPageSize = acsIdPageSize,
        acsIdFetchingParallelism = acsIdFetchingParallelism,
        acsContractFetchingParallelism = acsContractFetchingParallelism,
        acsGlobalParallelism = acsGlobalParallelism,
        acsIdQueueLimit = acsIdQueueLimit,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        lfValueTranslationCache = lfValueTranslationCache,
        enricher = Some(enricher),
        participantId = participantId,
        ledgerEndCache = ledgerEndCache,
        stringInterning = stringInterning,
        errorFactories = errorFactories,
        materializer = mat,
      )
  }
}

/** Computes the lag between the contract state events dispatcher and the general dispatcher.
  *
  * Internally uses a size bound for preventing memory leaks if misused.
  *
  * @param delegate The ledger head dispatcher delegate.
  * @param timer The timer measuring the delta.
  */
private[platform] class DispatcherLagMeter(delegate: SignalNewLedgerHead, maxSize: Long = 1000L)(
    timer: Timer
) extends SignalNewLedgerHead {
  private val ledgerHeads = mutable.Map.empty[Offset, Long]

  override def apply(offset: Offset, sequentialEventId: Long): Unit = {
    delegate(offset, sequentialEventId)
    ledgerHeads.synchronized {
      ledgerHeads.remove(offset).foreach { startNanos =>
        val endNanos = System.nanoTime()
        timer.update(endNanos - startNanos, TimeUnit.NANOSECONDS)
      }
    }
  }

  private[platform] def startTimer(head: Offset): Unit =
    ledgerHeads.synchronized {
      ensureBounded()
      discard(ledgerHeads.getOrElseUpdate(head, System.nanoTime()))
    }

  private def ensureBounded(): Unit =
    if (ledgerHeads.size > maxSize) {
      // If maxSize is reached, remove randomly ANY element.
      ledgerHeads.headOption.foreach(head => ledgerHeads.remove(head._1))
    } else ()
}
