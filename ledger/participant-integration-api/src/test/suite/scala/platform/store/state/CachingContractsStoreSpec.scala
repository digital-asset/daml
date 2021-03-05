package com.daml.platform.store.state

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{BoundedSourceQueue, Materializer}
import com.codahale.metrics.MetricRegistry
import com.daml.caching.SizedCache
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.{Identifier, Name, Party, TypeConName}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedValue
import com.daml.lf.value.Value.{ContractInst, ValueRecord, ValueText}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.dao.events.ContractLifecycleEventsReader.ContractStateEvent
import com.daml.platform.store.dao.events.{Contract, ContractId, ContractsReader}
import com.daml.platform.store.state.ContractsKeyCache.{Assigned, KeyStateUpdate}
import com.daml.platform.store.state.ContractsStateCache.ContractCacheValue
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

class CachingContractsStoreSpec
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with BeforeAndAfterAll
    with Eventually
    with ArgumentMatchersSugar {

  override implicit val executionContext: ExecutionContext =
    scala.concurrent.ExecutionContext.global

  private val contractsReaderMock = mock[ContractsReader]
  private val actorSystem = ActorSystem("test")
  private val templateId = TypeConName.assertFromString("somePackage:qualifier:test")
  private implicit val materializer: Materializer = Materializer(actorSystem)
  private implicit val testLoggingContext: LoggingContext = LoggingContext.ForTesting
  private val testCacheSize: SizedCache.Configuration = SizedCache.Configuration(1000L)
  private val contractsKeyCache: ContractsKeyCache =
    ContractsKeyCache(SizedCache.from[GlobalKey, KeyStateUpdate](testCacheSize))

  private val contractsStateCache = new ContractsStateCache(
    SizedCache.from[ContractId, ContractCacheValue](testCacheSize)
  )

  private val cachingContractsStore = new CachingContractsStore(
    store = contractsReaderMock,
    metrics = new Metrics(new MetricRegistry),
    keyCache = contractsKeyCache,
    contractsCache = contractsStateCache,
  )

  private val contractEventsStream = Source
    .queue[ContractStateEvent](16)
    .via(cachingContractsStore.consumeFrom)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  private val testEventSequentialId = new AtomicLong(0L)
  private val contractIdRef = new AtomicLong(0L)

  override def beforeAll(): Unit = {
    // Don't test divulgence
    when(
      contractsReaderMock.checkDivulgenceVisibility(*[ContractId], *[Set[Party]])(
        *[LoggingContext]
      )
    ).thenReturn(Future.successful(false))
    ()
  }

  private def indexReturnsNotFound(): Unit = {
    when(contractsReaderMock.lookupContractKey(*[GlobalKey], anyLong)(*[LoggingContext]))
      .thenReturn(Future.successful(Option.empty))
    when(contractsReaderMock.lookupContract(*[ContractId], anyLong)(*[LoggingContext]))
      .thenReturn(Future.successful(Option.empty))
    ()
  }

  override def afterAll(): Unit = {
    actorSystem.terminate()
    ()
  }

  private val defaultStakeholders = Set("party-1", "party-2")

  it should "work correctly in this simple scenario" in {
    // Initially we populate only from the contract events stream
    indexReturnsNotFound()

    val created1 = created(Some("global-key-1"), defaultStakeholders)
    val archived1 = archived(created1)

    for {
      _ <- assertCreated(contractEventsStream, created1)
      _ <- assertArchived(contractEventsStream, archived1)
      created2 = created(Some("global-key-1"), defaultStakeholders)
      _ <- assertCreated(contractEventsStream, created2)
      created3 = created(Some("global-key-2"), defaultStakeholders)
      _ <- assertCreated(contractEventsStream, created3)
      // ########### Assert cache population via read-throughs ###########
      _ <- `assert read-through`(created2)
    } yield succeed
  }

  it should "assert contract events stream population correctness in 1000 create and archive cycles" in {
    indexReturnsNotFound()
    `assert multiple create-archive cycles for the same key`("global-key-1", defaultStakeholders)(
      10000
    )
  }

  private def `assert read-through`(created: ContractStateEvent.Created): Future[Assertion] =
    withCacheContext {
      contractsKeyCache.cache.cache.invalidateAll() // Ensure the fetched element is not cached
      val readers = defaultStakeholders.map(party)
      when(
        contractsReaderMock.lookupContractKey(eqTo(created.globalKey.get), anyLong)(
          *[LoggingContext]
        )
      ).thenReturn(
        Future.successful(
          Some((created.contractId, created.flatEventWitnesses))
        )
      )

      for {
        lookupResult <- cachingContractsStore
          .lookupContractKey(readers, created.globalKey.get)
        _ = lookupResult shouldBe Some(created.contractId)
        _ <- eventually {
          contractsKeyCache.cache.cache.getIfPresent(created.globalKey.get) shouldBe Assigned(
            created.contractId,
            created.flatEventWitnesses,
          )
        }
      } yield succeed
    }

  private def `assert multiple create-archive cycles for the same key`(
      key: String,
      parties: Set[String],
  )(
      times: Int
  ): Future[Assertion] = {
    def createAndArchive: (ContractStateEvent.Created, ContractStateEvent.Archived) = {
      val c = created(Some(key), parties)
      (c, archived(c))
    }

    def createSingleLazyFuture
        : ((ContractStateEvent.Created, ContractStateEvent.Archived)) => Future[Assertion] = {
      case (c, a) =>
        for {
          _ <- assertCreated(contractEventsStream, c)
          _ <- assertArchived(contractEventsStream, a)
        } yield succeed
    }

    indexReturnsNotFound()
    val contracts = (1 to times).map(_ => createAndArchive)
    contracts.zip(contracts.tail).foldLeft(Future.successful(succeed)) {
      case (f, (eventsStream, indexState)) =>
        f.flatMap { _ =>
          // The index db reader will return one step in advance
          when(
            contractsReaderMock.lookupContractKey(
              eqTo(indexState._1.globalKey.get),
              eqTo(indexState._1.eventSequentialId),
            )(*[LoggingContext])
          )
            .thenReturn(
              Future.successful(Some((indexState._1.contractId, indexState._1.flatEventWitnesses)))
            )
          when(
            contractsReaderMock.lookupContractKey(
              eqTo(indexState._2.globalKey.get),
              eqTo(indexState._2.eventSequentialId),
            )(*[LoggingContext])
          )
            .thenReturn(Future.successful(None))
          when(
            contractsReaderMock.lookupContract(
              eqTo(indexState._1.contractId),
              eqTo(indexState._1.eventSequentialId),
            )(*[LoggingContext])
          )
            .thenReturn(
              Future.successful(
                Some(
                  (
                    indexState._1.contract,
                    indexState._1.flatEventWitnesses,
                    indexState._1.eventSequentialId,
                    None,
                  )
                )
              )
            )
          when(
            contractsReaderMock.lookupContract(
              eqTo(indexState._2.contractId),
              eqTo(indexState._2.eventSequentialId),
            )(*[LoggingContext])
          )
            .thenReturn(
              Future.successful(
                Some(
                  (
                    indexState._2.contract,
                    indexState._2.flatEventWitnesses,
                    indexState._2.createdAt,
                    Some(indexState._2.eventSequentialId),
                  )
                )
              )
            )
          createSingleLazyFuture(eventsStream)
        }
    }
  }

  private def assertCreated(
      contractEventsStream: BoundedSourceQueue[ContractStateEvent],
      createdEvent: ContractStateEvent.Created,
  ): Future[Assertion] =
    for {
      _ <- Future.successful(contractEventsStream.offer(createdEvent) shouldBe Enqueued)
      _ <- eventually {
        contractsKeyCache.cache.cache.getIfPresent(createdEvent.globalKey.get) shouldBe Assigned(
          createdEvent.contractId,
          createdEvent.flatEventWitnesses,
        )
      }
      _ <- withCacheContext {
        eventually {
          // Key Cache hit (assigned)
          cachingContractsStore
            .lookupContractKey(createdEvent.flatEventWitnesses, createdEvent.globalKey.get)
            .map(_ shouldBe Some(createdEvent.contractId))
        }
      }
      _ <- withCacheContext {
        eventually {
          // ContractId cache hit
          cachingContractsStore
            .lookupActiveContract(createdEvent.flatEventWitnesses, createdEvent.contractId)
            .map(_ shouldBe Some(createdEvent.contract))
        }
      }
      // Cache hit but no visibility for key
      _ <- withCacheContext {
        cachingContractsStore
          .lookupContractKey(Set("party-1337").map(party), createdEvent.globalKey.get)
          .map(_ shouldBe None)
      }

    } yield succeed

  private def assertArchived(
      contractEventsStream: BoundedSourceQueue[ContractStateEvent],
      archivedEvent: ContractStateEvent.Archived,
  ): Future[Assertion] =
    for {
      _ <- Future.successful(contractEventsStream.offer(archivedEvent) shouldBe Enqueued)
      _ <- withCacheContext {
        eventually {
          // Contract key de-assigned
          cachingContractsStore
            .lookupContractKey(
              archivedEvent.flatEventWitnesses,
              archivedEvent.globalKey.get,
            )
            .map(_ shouldBe None)
        }
      }

      _ <- withCacheContext {
        eventually {
          // Contract not active anymore
          cachingContractsStore
            .lookupActiveContract(archivedEvent.flatEventWitnesses, archivedEvent.contractId)
            .map(_ shouldBe None)
        }
      }
    } yield succeed

  private def withCacheContext[T](future: Future[T]): Future[T] =
    future.andThen { case Failure(_) =>
      println(s"Contracts key cache context: ${contractsKeyCache.cache.cache.asMap().asScala}")
      println(s"Contracts state cache context: ${contractsStateCache.cache.cache.asMap().asScala}")
    }

  private def created(
      maybeKey: Option[String],
      stakeholders: Set[String],
      cIdx: Long = contractIdRef.incrementAndGet(),
  ): ContractStateEvent.Created =
    ContractStateEvent.Created(
      contractId = contractId(cIdx),
      contract = contract(s"payload-$cIdx"),
      globalKey = maybeKey.map(globalKey),
      flatEventWitnesses = stakeholders.map(party),
      eventOffset = Offset.beforeBegin, // Not used
      eventSequentialId = testEventSequentialId.incrementAndGet(),
    )

  private def archived(
      created: ContractStateEvent.Created
  ): ContractStateEvent.Archived =
    ContractStateEvent.Archived(
      contractId = created.contractId,
      contract = created.contract,
      globalKey = created.globalKey,
      flatEventWitnesses = created.flatEventWitnesses,
      createdAt = created.eventSequentialId,
      eventOffset = Offset.beforeBegin, // Not used
      eventSequentialId = testEventSequentialId.incrementAndGet(),
    )

  private def contractId(idx: Long): ContractId =
    ContractId.assertFromString(s"#contract-$idx")

  private def contract(payload: String): Contract =
    ContractInst(
      templateId,
      assertAsVersionedValue(
        ValueRecord(
          Some(Identifier.assertFromString("somePackage:qualifier:test")),
          ImmArray((Some[Name](Name.assertFromString("p")), ValueText(payload))),
        )
      ),
      "",
    )

  private def globalKey(desc: String): GlobalKey =
    GlobalKey.assertBuild(templateId, ValueText(desc))

  private def party(name: String): Ref.Party = Ref.Party.assertFromString(name)
}
