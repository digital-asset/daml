package com.daml.http

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.dbutils.JdbcConfig
import com.daml.jwt.domain.Jwt
import com.daml.http.ContractsService
import com.daml.ledger.api.{domain => LedgerApiDomain}
import org.scalatest.{Assertion, BeforeAndAfterAll, EitherValues, Inside, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.fetchcontracts.domain.TemplateId
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.dbbackend.DbStartupMode.CreateAndStart
import com.daml.http.dbbackend.ContractDao
import com.daml.http.domain.{ApplicationId, GetActiveContractsRequest, LedgerId, OkResponse, Party}
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface
import com.daml.logging.LoggingContextOf.{label, newLoggingContext}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Metrics
import scalaz.{NonEmptyList, OneAnd, \/, \/-}
import spray.json._
import DefaultJsonProtocol._
import org.testcontainers.containers.JdbcDatabaseContainer

import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.immutable.Seq

//import scala.language.existentials
import scala.concurrent.{ExecutionContext, Future}

class ContractServiceOnOracleTest extends ContractServiceTest {
  override val container = new org.testcontainers.containers.OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
    .withStartupTimeout(java.time.Duration.ofMinutes(10))
}

class ContractServiceOnPostgresTest extends ContractServiceTest {
  override val container = new org.testcontainers.postgresql.PostgreSQLContainer("postgres")

}


@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class ContractServiceTest extends AsyncWordSpec with Matchers with Inside with BeforeAndAfterAll with EitherValues with OptionValues {
  private implicit val ec: ExecutionContext =
    ExecutionContext.global
  private implicit val system: ActorSystem = ActorSystem("ContractServiceTest")
  private implicit val materializer: Materializer = Materializer(system)

  protected val container: JdbcDatabaseContainer[_]

  implicit val metrics: Metrics = new Metrics(new MetricRegistry())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
  }

  override protected def afterAll(): Unit = {
    system.terminate()
    container.stop()
    super.afterAll()
  }

  private def createDao(): ContractDao = {

    val jdbcUrl = container.getJdbcUrl()
    val username = container.getUsername()
    val password = container.getPassword()

    val baseConfig = JdbcConfig(
      driver = container.getDriverClassName,
      url = jdbcUrl,
      user = username,
      password = password,
      poolSize = 7,
      //more
    )
    val jdbcConfig = com.daml.http.dbbackend.JdbcConfig(
      baseConfig = baseConfig,
      startMode = CreateAndStart,
      backendSpecificConf = Map.empty,
    )

    ContractDao(
      jdbcConfig,
      tpIdCacheMaxEntries = Some(5),
    )
  }

  private def createService(ledger: LedgerState, contractDao: ContractDao): ContractsService = {

    val contractsService = new ContractsService(
      resolveContractTypeId = ledger.resolveTemplate,
      resolveTemplateId = ledger.resolveTemplate,
      allTemplateIds = ledger.allTemplateIds,
      getActiveContracts = ledger.getActiveContracts,
      getCreatesAndArchivesSince = ledger.getCreatesAndArchivesSince,
      getTermination = ledger.termination,
      lookupType = ledger.lookupType,
      contractDao = Some(contractDao),
    )
    contractsService
  }

  private def searchRequest(service: ContractsService, parties: Seq[String])(implicit
                                                                             lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Future[Seq[PseudoContract]] = {

    val p = parties.map(Party(_))
    val request: GetActiveContractsRequest = GetActiveContractsRequest(
      templateIds = OneAnd(LedgerState.templateA_OptId, Set.empty),
      query = Map.empty,
      readAs = Some(NonEmptyList(p.head, p.tail: _*)),
    )
    val jwt = Jwt("fake token")
    val jwtPayload = domain.JwtPayload(
      LedgerId("ledgerId"),
      ApplicationId("applicationId"),
      readAs = p.toList,
      actAs = p.toList,
    ).value

    for {
      response <- service.search(jwt, jwtPayload, request)
      results <- response match {
        case OkResponse(res, _, _) =>
          val stream: Source[ContractsService.Error \/ domain.ActiveContract[JsValue], NotUsed] = res
          stream.runFold(List.empty[ContractsService.Error \/ domain.ActiveContract[JsValue]]) {
            (acc, elem) => acc :+ elem
          }
        case other =>
          Future.failed(new RuntimeException(s"Unexpected response: $other"))
      }
      (contracts, errors) = results.partition {
        case \/-(_) => true
        case _ => false
      }
      _ <- if (errors.nonEmpty) {
        Future.failed(new RuntimeException(s"Errors fetching contracts: $errors"))
      } else {
        Future.unit
      }
    } yield contracts.collect {
      case \/-(ac) => ac
    }.map(v =>
      PseudoContract(
        v.contractId.toString,
        v.payload.asJsObject.fields("v").convertTo[String].toInt,
        v.signatories.map(_.toString)))
  }

  "contract service tests" should {
    "initialize and connect to database" ignore {

      val contractDao = createDao()
      val ledgerState = new LedgerState()
      ledgerState.init(1)(
        Seq(
          ledgerState.create(1)(42),
          ledgerState.create(2)(142),
        )
      )
      val contractsService = createService(ledgerState, contractDao)

      implicit val ignoredLoggingContext
      : LoggingContextOf[InstanceUUID with RequestID] =
        newLoggingContext(label[InstanceUUID with RequestID])(identity)

      for {
        _ <- DbStartupOps.fromStartupMode(contractDao, CreateAndStart).unsafeToFuture()
        response1 <- searchRequest(contractsService, Seq(TestParties.alice))
        _ <- Future {
          ledgerState.trx(2)(
            Seq(
              ledgerState.archive(1),
              ledgerState.create(3)(242),
            )
          )
        }
        response2 <- searchRequest(contractsService, Seq(TestParties.alice))
      } yield {
        response1 should contain theSameElementsInOrderAs ledgerState.acsAt(1, Seq(TestParties.alice))
        response2 should contain theSameElementsInOrderAs ledgerState.acsAt(2, Seq(TestParties.alice))
      }
    }

    "bug 0" ignore  {

      val contractDao = createDao()
      val ledgerState = new LedgerState()
      ledgerState.init(1)(
        Seq(
          ledgerState.create(10, Seq(TestParties.alice))(7),
          ledgerState.create(20, Seq(TestParties.alice, TestParties.bob))(17),
        )
      )
      val contractsService = createService(ledgerState, contractDao)

      implicit val ignoredLoggingContext
      : LoggingContextOf[InstanceUUID with RequestID] =
        newLoggingContext(label[InstanceUUID with RequestID])(identity)
      ledgerState.moveOffset(30)

      for {
        _ <- DbStartupOps.fromStartupMode(contractDao, CreateAndStart).unsafeToFuture()
        response1 <- searchRequest(contractsService, Seq(TestParties.bob))
        _ <- Future {
          ledgerState.moveOffset(40)
        }
        response2 <- searchRequest(contractsService, Seq(TestParties.alice))
      } yield {
        response1 should contain theSameElementsInOrderAs ledgerState.acsAt(30, Seq(TestParties.bob))
        response2 should contain theSameElementsInOrderAs ledgerState.acsNow(Seq(TestParties.alice))
      }
    }

    "the bug" ignore  {

      val contractDao = createDao()
      val ledgerState = new LedgerState()
      ledgerState.init(1)(
        Seq(
          ledgerState.create(10, Seq(TestParties.alice))(7),
          ledgerState.create(20, Seq(TestParties.alice))(17),
        )
      )
      val contractsService = createService(ledgerState, contractDao)

      implicit val ignoredLoggingContext
      : LoggingContextOf[InstanceUUID with RequestID] =
        newLoggingContext(label[InstanceUUID with RequestID])(identity)
      ledgerState.moveOffset(20)

      for {
        _ <- DbStartupOps.fromStartupMode(contractDao, CreateAndStart).unsafeToFuture()
        response1 <- searchRequest(contractsService, Seq(TestParties.alice))
        _ <- Future {
          ledgerState.trx(30)(
            Seq[PseudoEvent](
              ledgerState.create(30, Seq(TestParties.alice))(616),
              ledgerState.archive(30, Seq(TestParties.alice)),
            )
          )
        }
        response2 <- searchRequest(contractsService, Seq(TestParties.alice))
      } yield {
        response1 should contain theSameElementsInOrderAs ledgerState.acsAt(20, Seq(TestParties.alice))
        response2 should contain theSameElementsInOrderAs ledgerState.acsNow(Seq(TestParties.alice))
        fail("test fail")
      }
    }

    "randomize" in {
      implicit val ignoredLoggingContext
      : LoggingContextOf[InstanceUUID with RequestID] =
        newLoggingContext(label[InstanceUUID with RequestID])(identity)
      val contractDao = createDao()

      val ledgerState = new LedgerState()
      val parties = Seq(TestParties.alice)
      ledgerState.init(1)(
        Seq(
          ledgerState.create(signatories=  parties)(1),
          ledgerState.create(signatories =  parties)(11),
        )
      )
      val contractsService = createService(ledgerState, contractDao)
      val executor = Executors.newFixedThreadPool(10)
      val eexecutorEc: ExecutionContext = ExecutionContext.fromExecutorService(executor)

      def task(n: Int) =  {
        val rnd = new scala.util.Random(40)
        val res = (1 to n).map(_  =>Future {
          val acs = ledgerState.acsNow(parties)
          val toArchive: Seq[PseudoEvent] = acs.filter(_ => rnd.nextBoolean()).map(
            c => ledgerState.archiveContract(
              contractId = c.contractId,
              witness = parties,
            )
          )
          val toCreate: Seq[PseudoEvent] = if (rnd.nextBoolean()) {
            Seq(ledgerState.create(
              signatories = parties
            )(111))
          } else Seq.empty

          val (_, state) = ledgerState.trx()(toArchive ++ toCreate)
          for {
            result <- searchRequest(contractsService, parties)
          } yield {
            val archived = ledgerState.archivedAt(state.ledgerOffset, parties)
            val capturedState = ledgerState.peek()
            val activeContracts = result.map(_.contractId)
            val zombie = activeContracts.intersect(archived)
            if (zombie.nonEmpty) {
              println("jarekr: archived: " + archived.mkString(", "))
              println("jarekr: active: " + activeContracts.mkString(", "))
              println(s"jarekr: ledger: $capturedState")
              fail(s"jarekr: found zombies: $zombie")
            }
            zombie should be(empty)
          }
        }(eexecutorEc))
        val x: Future[IndexedSeq[Assertion]] = Future.sequence(res.map( _.flatten))
        x
      }

      for {
        _ <- DbStartupOps.fromStartupMode(contractDao, CreateAndStart).unsafeToFuture()
        _ <- {
          val res = (1 to 150).map(_ => task(30))
          Future.sequence(res)
        }
      } yield succeed
    }
  }

}


case class PseudoContract(contractId: String, v: Int, signatories: Seq[String])

sealed trait PseudoEvent extends Product with Serializable

final case class PseudoCreatedEvent(contract: PseudoContract) extends PseudoEvent

final case class PseudoArchiveEvent(contractId: String, witnessParties: Seq[String]) extends PseudoEvent

case class PseudoTransaction(offset: Long, events: Seq[PseudoEvent])


case class PseudoLedger(ledgerOffset: Long, transactions: Seq[PseudoTransaction]) {
  def addTransaction(t: PseudoTransaction): PseudoLedger = {
    this.copy(
      ledgerOffset = t.offset,
      transactions = transactions :+ t,
    )
  }

  def filterEvents(beginExclusiveOffset: Long, endInclusiveOffset: Long, parties: Seq[String]): Seq[PseudoEvent] = {
    transactions.filter(t => t.offset > beginExclusiveOffset && t.offset <= endInclusiveOffset)
      .flatMap(_.events)
      .filter {
        case PseudoCreatedEvent(c) => c.signatories.exists(parties.contains)
        case PseudoArchiveEvent(_, witnessParties) => witnessParties.exists(parties.contains)
      }
  }

  def filterTransactions(beginExclusiveOffset: Long, endInclusiveOffset: Long, parties: Seq[String]): Seq[PseudoTransaction] = {
    transactions.filter(t => t.offset > beginExclusiveOffset && t.offset <= endInclusiveOffset)
      .map(t => t.copy(
        events = t.events.filter {
          case PseudoCreatedEvent(c) => c.signatories.exists(parties.contains)
          case PseudoArchiveEvent(_, witnessParties) => witnessParties.exists(parties.contains)
        }
      ))
      .filter(t => t.events.nonEmpty)
  }

  def calculateActiveAtOffset(offset: Long, parties: Seq[String]): Seq[PseudoCreatedEvent] = {
    val events = filterEvents(0, offset, parties)
    val activeContracts = events.foldLeft(Seq.empty[PseudoCreatedEvent]) {
      case (acs, e) => e match {
        case ce@PseudoCreatedEvent(_) => acs :+ ce
        case PseudoArchiveEvent(cid, _) => acs.filterNot(_.contract.contractId == cid)
      }
    }
    activeContracts
  }

  def acsNow(parties: Seq[String]): Seq[PseudoContract] = acsAt(ledgerOffset, parties)

  def acsAt(offset: Long, parties: Seq[String]): Seq[PseudoContract] = {
    calculateActiveAtOffset(offset, parties).map(_.contract)
  }

  def archivedAt(ledgerOffset: Long, parties: Seq[String]): Seq[String] = {
    filterEvents(0, ledgerOffset, parties).collect {
      case PseudoArchiveEvent(cid, _) => cid
    }
  }

}

class LedgerState {
  private val ledger: AtomicReference[PseudoLedger] = new AtomicReference(PseudoLedger(0, Seq.empty))
  private val nextOffset: AtomicLong = new AtomicLong(1)
  private val nextCid: AtomicLong = new AtomicLong(1)

  def peek() : PseudoLedger = ledger.get()

  def create(
              cid: Long = nextCid.getAndIncrement(),
              signatories: Seq[String] = Seq(TestParties.alice, TestParties.bob),
            )
            (v: Long)
  : PseudoCreatedEvent = {
    val contractId = toCid(cid)
    PseudoCreatedEvent(PseudoContract(contractId, v.toInt, signatories))
  }

  private def toCid(id: Long ):String= s"contractId_($id)"

  def archive(cid: Long, witness: Seq[String] = Seq(TestParties.alice, TestParties.bob)): PseudoArchiveEvent = {
    PseudoArchiveEvent(toCid(cid), witness)
  }

  def archiveContract(contractId: String, witness: Seq[String] = Seq(TestParties.alice, TestParties.bob)): PseudoArchiveEvent = {
    PseudoArchiveEvent(contractId, witness)
  }


  //mutates
  def trx(
           offset: Long = nextOffset.getAndIncrement()
         )(events: Seq[PseudoEvent]): (PseudoTransaction, PseudoLedger) = {
    val transaction = PseudoTransaction(offset, events)
    val state = ledger.updateAndGet(l => l.addTransaction(transaction))
    (transaction, state)
  }

  def moveOffset(v: Long): PseudoLedger = {
    nextOffset.set(v + 1)
    ledger.updateAndGet(l => l.copy(ledgerOffset = v))
  }

  def init(
            offset: Long = nextOffset.getAndIncrement()
          )(created: Seq[PseudoCreatedEvent]): PseudoLedger = {
    val transaction = PseudoTransaction(offset, created)
    val initialLedger = PseudoLedger(offset, Seq(transaction))
    ledger.set(initialLedger)
    initialLedger
  }: PseudoLedger

  def acsNow(parties: Seq[String]): Seq[PseudoContract] = {
    ledger.get().acsNow(parties)
  }

  def acsAt(offset: Long, parties: Seq[String]): Seq[PseudoContract] = {
    ledger.get().acsAt(offset, parties)
  }

  private def toCreatedEvent(e: PseudoCreatedEvent): com.daml.ledger.api.v1.event.CreatedEvent = {
    val c = e.contract
    com.daml.ledger.api.v1.event.CreatedEvent(
      eventId = s"eventId_${c.contractId}",
      contractId = c.contractId,
      templateId = Some(LedgerState.templateA),
      createArguments = Some(
        com.daml.ledger.api.v1.value.Record(
          fields = Seq(
            com.daml.ledger.api.v1.value.RecordField(
              label = "v",
              value = Some(com.daml.ledger.api.v1.value.Value().withInt64(c.v.toLong)),
            )
          )
        )
      ),
      signatories = c.signatories,
    )
  }

  private def toArchivedEvent(e: PseudoArchiveEvent): com.daml.ledger.api.v1.event.ArchivedEvent = {
    com.daml.ledger.api.v1.event.ArchivedEvent(
      eventId = s"eventId_${e.contractId}",
      contractId = e.contractId,
      templateId = Some(LedgerState.templateA),
    )
  }

  private def toEvent(e: PseudoEvent): com.daml.ledger.api.v1.event.Event = e match {
    case ce@PseudoCreatedEvent(_) => com.daml.ledger.api.v1.event.Event(
      event = Created(
        toCreatedEvent(ce)
      )
    )
    case ae@PseudoArchiveEvent(_, _) => com.daml.ledger.api.v1.event.Event(
      event = com.daml.ledger.api.v1.event.Event.Event.Archived(
        toArchivedEvent(ae)
      )
    )
  }

  private def toTransaction(t: PseudoTransaction): Transaction = {
    val events = t.events.map(toEvent)
    Transaction(
      transactionId = "transactionId1",
      commandId = "",
      workflowId = "",
      events = events,
      offset = "0002",
    )
  }

  val getActiveContracts: LedgerClientJwt.GetActiveContracts = { (_: Jwt, _: LedgerApiDomain.LedgerId, filter: TransactionFilter, _: Boolean) =>
    (_: LoggingContextOf[InstanceUUID]) => {
      println(s"jarekr: getActiveContracts called")
      val state = ledger.get()
      val acs = state.calculateActiveAtOffset(state.ledgerOffset.toLong, filter.filtersByParty.keys.toSeq)
      val created = acs.map(toCreatedEvent)

      val r1 = GetActiveContractsResponse(
        offset = "0001",
        workflowId = "",
        activeContracts = created,
      )
      Source.single(r1)
    }
  }
  val getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince = {
    (
      _: Jwt,
      _: LedgerApiDomain.LedgerId,
      filter: TransactionFilter,
      offset: LedgerOffset,
      endOffset: Terminates,
    ) => { (_: LoggingContextOf[InstanceUUID]) =>
      println(s"jarekr: getCreatesAndArchivesSince called")
      val ledgetState = ledger.get()
      val transactions = ledgetState.filterTransactions(
        beginExclusiveOffset = offset.getAbsolute.toLong,
        endInclusiveOffset = endOffset match {
          case Terminates.AtAbsolute(lo) => lo.value.toLong
          case _ => ledgetState.ledgerOffset
        },
        parties = filter.filtersByParty.keys.toSeq,
      )
      val txs = transactions.map(toTransaction)

      Source.fromIterator(() => txs.iterator)

    }
  }
  val termination: LedgerClientJwt.GetTermination = { (_: Jwt, _: LedgerApiDomain.LedgerId) =>
    (_: LoggingContextOf[InstanceUUID]) => {
      val ledgerState = ledger.get()
      Future.successful(Some(Terminates.AtAbsolute(LedgerOffset.Value.Absolute(ledgerState.ledgerOffset.toString))))
    }
  }
  val resolveTemplate: PackageService.ResolveContractTypeId = { _: LoggingContextOf[InstanceUUID] => {
    (_: Jwt, _: LedgerApiDomain.LedgerId) => {
      _: TemplateId.OptionalPkg => Future.successful(\/-(Some(LedgerState.templateA_id)))
    }
  }
  }

  val lookupType: query.ValuePredicate.TypeLookup = { rid =>
    println(s"jarekr: lookupType $rid")
    val record = iface.DefDataType(
      ImmArraySeq.empty,
      iface.Record(
        ImmArraySeq(
          (Ref.Name.assertFromString("v"), iface.TypePrim(iface.PrimType.Int64, ImmArraySeq()))
        )
      ),
    )
    Some(record)
  }


  val allTemplateIds: PackageService.AllTemplateIds = _ => {
    (_: Jwt, _: LedgerApiDomain.LedgerId) =>
      println(s"jarekr: allTemplateIds called")
      Future.successful(
        Set(
          LedgerState.templateA_id
        )
      )
  }

  def archivedAt(ledgerOffset: Long, parties: Seq[String]): Seq[String] = {
    ledger.get().archivedAt(ledgerOffset, parties)
  }
}

object LedgerState {
  val templateA = com.daml.ledger.api.v1.value.Identifier(
    packageId = "testPackage",
    moduleName = "testModule",
    entityName = "TemplateA",
  )

  val templateA_id = TemplateId[String](templateA.packageId,
    templateA.moduleName,
    templateA.entityName)

  val templateA_OptId: TemplateId.OptionalPkg = TemplateId(
    Some(templateA.packageId), templateA.moduleName, templateA.entityName)
}

object TestParties {
  val alice: String = "Alice"
  val bob: String = "Bob"
  val charlie: String = "Charlie"
}