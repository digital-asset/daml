package com.daml.http

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import cats.effect.{ContextShift, IO}
import com.codahale.metrics.MetricRegistry
import com.daml.dbutils.ConnectionPool
import com.daml.dbutils.JdbcConfig
import com.daml.jwt.domain.Jwt
import com.daml.http.ContractsService
import com.daml.ledger.api.{domain => LedgerApiDomain}
import org.scalatest.{BeforeAndAfterAll, EitherValues, Inside, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.fetchcontracts.domain.TemplateId
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.dbbackend.DbStartupMode.CreateAndStart
import com.daml.http.dbbackend.{ContractDao, SupportedJdbcDriver}
import com.daml.http.domain.{ApplicationId, GetActiveContractsRequest, LedgerId, OkResponse, Party}
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref
import com.daml.lf.iface
import com.daml.logging.LoggingContextOf.{label, newLoggingContext}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Metrics
import org.testcontainers.containers.PostgreSQLContainer
import scalaz.{NonEmptyList, OneAnd, \/, \/-}
import spray.json.JsValue

import scala.language.existentials
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ContractServiceTest extends AsyncWordSpec with Matchers with Inside with BeforeAndAfterAll with EitherValues with OptionValues{
  private implicit val ec: ExecutionContext =
    ExecutionContext.global
  private implicit val system: ActorSystem = ActorSystem("ContractServiceTest")
  private implicit val materializer: Materializer = Materializer(system)

  private val container = new PostgreSQLContainer("postgres")

  // JDBC URL and credentials will be available in withContainers { container => ... }

  override def afterAll(): Unit = {
    system.terminate()
    container.stop()
    super.afterAll()
  }

  "contract service tests" should {
    "initialize and connect to database" in {
      implicit val metrics: Metrics = new Metrics(new MetricRegistry())
      container.start()

      val jdbcUrl = container.getJdbcUrl()
      val username = container.getUsername()
      val password = container.getPassword()
      println(jdbcUrl)

      val baseConfig = JdbcConfig(
        driver = "org.postgresql.Driver",
        url = jdbcUrl,
        user = username,
        password = password,
        poolSize = 10,
        //more
      )
      implicit val driver: SupportedJdbcDriver.TC = SupportedJdbcDriver.Postgres
        .configure(tablePrefix = baseConfig.tablePrefix, extraConf = Map.empty, tpIdCacheMaxEntries = 10).value

      val cs: ContextShift[IO] = IO.contextShift(ec)
      val es = Executors.newWorkStealingPool(baseConfig.poolSize)
      val (ds, conn) =
        ConnectionPool.connect(baseConfig)(ExecutionContext.fromExecutor(es), cs)
      val contractDao = new ContractDao(ds, conn, es)

      //mocks
      val resolveTemplate: PackageService.ResolveContractTypeId = { _: LoggingContextOf[InstanceUUID] => {
        (_: Jwt, _: LedgerApiDomain.LedgerId) => {
          _: TemplateId.OptionalPkg => Future.successful(\/-(Some(TemplateId[String]("packageId", "moduleName", "TemplateA"))))
        }
      }
      }
      val termination: LedgerClientJwt.GetTermination = { (_: Jwt, _: LedgerApiDomain.LedgerId) =>
      (_: LoggingContextOf[InstanceUUID]) => {
          Future.successful(Some(Terminates.AtAbsolute(LedgerOffset.Value.Absolute("end")) ))
      }
      }

//      val dummyPackageId = Ref.PackageId.assertFromString("dummy-package-id")
//      val dummyId = Ref.Identifier(
//        dummyPackageId,
//        Ref.QualifiedName.assertFromString("Foo:Bar"),
//      )
//      val dummyTypeCon = iface.TypeCon(iface.TypeConName(dummyId), ImmArraySeq.empty)

      val record =  iface.DefDataType(
        ImmArraySeq.empty,
        iface.Record(
          ImmArraySeq(
            (Ref.Name.assertFromString("v"), iface.TypePrim(iface.PrimType.Int64, ImmArraySeq()))
          )
        ),
      )

//      val predicate: ValuePredicate = ValuePredicate.fromJsObject(
//        Map("v" -> JsNumber(0)),
//        dummyTypeCon,
//        Map(
//          dummyId -> record
//        ).lift,
//      )

      val lookupType: query.ValuePredicate.TypeLookup = { rid  =>
        println(s"jarekr: lookupType $rid")
        Some(record)
      }
      val allTemplateIds : PackageService.AllTemplateIds = _ => {
        (_:Jwt, _:LedgerApiDomain.LedgerId) =>
          println(s"jarekr: allTemplateIds called")
          Future.successful(
            Set(
              TemplateId[String]("packageId", "moduleName", "TemplateA")
            )
          )
      }

      val getActiveContracts: LedgerClientJwt.GetActiveContracts = { (_: Jwt, _: LedgerApiDomain.LedgerId,_: TransactionFilter, _:Boolean) =>
        (_: LoggingContextOf[InstanceUUID]) => {
          println(s"jarekr: getActiveContracts called")
          Source.empty
        }
      }

      val getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince = {
        (
          _:Jwt,
          _:LedgerApiDomain.LedgerId,
          _:TransactionFilter,
          _:LedgerOffset,
          _: Terminates,
        ) => { (_: LoggingContextOf[InstanceUUID]) =>
          println(s"jarekr: getCreatesAndArchivesSince called")
          Source.empty
        }
      }

      val contractsService = new ContractsService(
        resolveContractTypeId = resolveTemplate,
        resolveTemplateId = resolveTemplate,
        allTemplateIds = allTemplateIds,
        getActiveContracts = getActiveContracts,
        getCreatesAndArchivesSince = getCreatesAndArchivesSince,
        getTermination = termination,
        lookupType = lookupType,
        contractDao = Some(contractDao),
      )

      println("jarekr")
      println(contractsService)
      val partyA = Party("partyA")
      val templateId:TemplateId.OptionalPkg =  TemplateId(Some("packageId"), "moduleName", "TemplateA")
      val request:GetActiveContractsRequest = GetActiveContractsRequest(
        templateIds = OneAnd(templateId,Set.empty),
        query = Map.empty,
        readAs = Some(NonEmptyList(partyA))
      )
      val jwt = Jwt("fake token")
      val jwtPayload = domain.JwtPayload(
        LedgerId("ledgerId"),
        ApplicationId("applicationId"),
        readAs = List(partyA),
        actAs = List(partyA),
      ).value

      implicit val ignoredLoggingContext
      : LoggingContextOf[InstanceUUID with RequestID] =
        newLoggingContext(label[InstanceUUID with RequestID])(identity)

      for {
        initialized: Boolean <- DbStartupOps.fromStartupMode(contractDao, CreateAndStart).unsafeToFuture()
        _ <- Future {
          println(s"jarekr: DB initialized: $initialized")
        }
        response <- contractsService.search(jwt, jwtPayload, request)
        result <- response match {
          case OkResponse(res, warns, status) =>
            println(s"jarker: got response source $res $status $warns" )
            val x: Source[ContractsService.Error \/ domain.ActiveContract[JsValue], NotUsed] = res
            x.runFold(List.empty[ContractsService.Error \/ domain.ActiveContract[JsValue]]) {
              (acc, elem) => acc :+ elem
            }
          case err=>
            Future.failed(new RuntimeException(s"Error fetching contracts: $err"))

        }
      } yield {

        println("result")
        println(result)
        fail("unimplemented test")
      }
    }


  }
}
