package com.daml.platform.index

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.store.appendonlydao.EventSequentialId
import com.daml.platform.store.cache.MutableCacheBackedContractStore
import com.daml.platform.store.dao.LedgerReadDao
import com.daml.platform.store.dao.events.ContractStateEvent

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private[index] object ReadOnlySqlLedgerWithMutableContractStateCache {

  final class Owner(
      ledgerDao: LedgerReadDao,
      ledgerId: LedgerId,
      metrics: Metrics,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[ReadOnlySqlLedgerWithMutableContractStateCache] {

    override def acquire()(implicit
        context: ResourceContext
    ): Resource[ReadOnlySqlLedgerWithMutableContractStateCache] =
      for {
        (ledgerEndOffset, ledgerEndSequentialId) <- Resource.fromFuture(
          ledgerDao.lookupLedgerEndOffsetAndSequentialId()
        )
        contractStateEventsDispatcher <- dispatcherOffsetSeqIdOwner(
          ledgerEndOffset,
          ledgerEndSequentialId,
        ).acquire()
        generalDispatcher <- dispatcherOwner(ledgerEndOffset).acquire()
        contractStore <- contractStoreOwner(generalDispatcher)
        ledger <- ledgerOwner(contractStateEventsDispatcher, generalDispatcher, contractStore)
          .acquire()
      } yield ledger

    private def ledgerOwner(
        contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        contractStore: MutableCacheBackedContractStore,
    ) = {
      ResourceOwner
        .forCloseable(() =>
          new ReadOnlySqlLedgerWithMutableContractStateCache(
            ledgerId,
            ledgerDao,
            contractStore,
            contractStateEventsDispatcher,
            generalDispatcher,
          )
        )
    }

    private def contractStoreOwner(generalDispatcher: Dispatcher[Offset])(implicit
        context: ResourceContext
    ) =
      MutableCacheBackedContractStore.owner(
        contractsReader = ledgerDao.contractsReader,
        signalNewLedgerHead = generalDispatcher.signalNewHead,
        metrics = metrics,
        maxContractsCacheSize = maxContractStateCacheSize,
        maxKeyCacheSize = maxContractKeyStateCacheSize,
      )

    private def dispatcherOffsetSeqIdOwner(ledgerEnd: Offset, evtSeqId: Long) = {
      implicit val ordering: Ordering[(Offset, Long)] = Ordering.fromLessThan {
        case ((fOffset, fSeqId), (sOffset, sSeqId)) =>
          (fOffset < sOffset) || (fOffset == sOffset && fSeqId < sSeqId)
      }
      Dispatcher.owner(
        name = "contract-state-events",
        zeroIndex = (Offset.beforeBegin, EventSequentialId.BeforeBegin),
        headAtInitialization = (ledgerEnd, evtSeqId),
      )
    }

    private def dispatcherOwner(ledgerEnd: Offset): ResourceOwner[Dispatcher[Offset]] =
      Dispatcher.owner(
        name = "sql-ledger",
        zeroIndex = Offset.beforeBegin,
        headAtInitialization = ledgerEnd,
      )
  }
}

private final class ReadOnlySqlLedgerWithMutableContractStateCache(
    ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    contractStore: MutableCacheBackedContractStore,
    contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
    dispatcher: Dispatcher[Offset],
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends ReadOnlySqlLedger(ledgerId, ledgerDao, contractStore, dispatcher) {
  private val logger = ContextualizedLogger.get(getClass)

  protected val (ledgerEndUpdateKillSwitch, ledgerEndUpdateDone) =
    RestartSource
      .withBackoff(
        RestartSettings(minBackoff = 1.second, maxBackoff = 10.seconds, randomFactor = 0.2)
      )(() =>
        Source
          .tick(0.millis, 100.millis, ())
          .mapAsync(1)(_ => ledgerDao.lookupLedgerEndOffsetAndSequentialId())
      )
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.foreach(contractStateEventsDispatcher.signalNewHead))(
        Keep.both[UniqueKillSwitch, Future[Done]]
      )
      .run()

  private val (contractStateUpdateKillSwitch, contractStateUpdateDone) =
    contractStateEvents
      .map(_._2)
      .via(contractStore.consumeFrom)
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.ignore)(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

  contractStateUpdateDone
    .onComplete { // TDT No bueno, it dies silently
      case Failure(exception) =>
        logger.error("Event state consumption stream failed", exception)
      case Success(_) =>
        logger.info("Finished consuming state events")
    }(mat.executionContext)

  private def contractStateEvents(implicit
      loggingContext: LoggingContext
  ): Source[((Offset, Long), ContractStateEvent), NotUsed] =
    contractStateEventsDispatcher.startingAt(
      Offset.beforeBegin -> EventSequentialId.BeforeBegin, // Always start from initialization head
      RangeSource(
        ledgerDao.transactionsReader.getContractStateEvents(_, _)
      ),
    )

  override def close(): Unit = {
    contractStateEventsDispatcher.close() // TDT why not handle with a resource?
    contractStateUpdateKillSwitch.shutdown()
    Await.result(contractStateUpdateDone, 10.seconds)

    super.close()
  }
}
