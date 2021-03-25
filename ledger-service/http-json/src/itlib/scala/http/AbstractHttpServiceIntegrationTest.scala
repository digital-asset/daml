// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.security.DigestInputStream
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import com.daml.api.util.TimestampConversion
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.lf.data.Ref
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.domain.ContractId
import com.daml.http.domain.TemplateId.OptionalPkg
import com.daml.http.json.SprayJson.{decode, decode1, objectField}
import com.daml.http.json._
import com.daml.http.util.ClientUtil.{boxedRecord, uniqueId}
import com.daml.http.util.FutureUtil.toFuture
import com.daml.http.util.{FutureUtil, TestUtil}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.service.MetadataReader
import com.daml.ledger.test.ModelTestDar
import com.daml.platform.participant.util.LfEngineToApi
import com.typesafe.scalalogging.StrictLogging
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.syntax.bitraverse._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{-\/, \/, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

object AbstractHttpServiceIntegrationTestFuns {
  private[http] val dar1 = requiredResource("docs/quickstart-model.dar")

  private[http] val dar2 = requiredResource("ledger-service/http-json/Account.dar")

  private[http] val dar3 = requiredResource(ModelTestDar.path)

  def sha256(source: Source[ByteString, Any])(implicit mat: Materializer): Try[String] = Try {
    import java.security.MessageDigest
    import javax.xml.bind.DatatypeConverter

    val md = MessageDigest.getInstance("SHA-256")
    val is = source.runWith(StreamConverters.asInputStream())
    val dis = new DigestInputStream(is, md)

    // drain the input stream and calculate the hash
    while (-1 != dis.read()) ()

    dis.on(false)

    DatatypeConverter.printHexBinary(md.digest()).toLowerCase
  }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait AbstractHttpServiceIntegrationTestFuns extends StrictLogging {
  this: AsyncTestSuite with Matchers with Inside =>
  import AbstractHttpServiceIntegrationTestFuns._
  import json.JsonProtocol._
  import HttpServiceTestFixture._

  def jdbcConfig: Option[JdbcConfig]

  def staticContentConfig: Option[StaticContentConfig]

  def useTls: UseTls

  def wsConfig: Option[WebsocketConfig]

  protected def testId: String = this.getClass.getSimpleName

  protected val metadata2: MetadataReader.LfMetadata =
    MetadataReader.readFromDar(dar2).valueOr(e => fail(s"Cannot read dar2 metadata: $e"))

  protected val jwt: Jwt = jwtForParties(List("Alice"), List(), testId)

  protected val jwtAdminNoParty: Jwt = {
    val decodedJwt = DecodedJwt(
      """{"alg": "HS256", "typ": "JWT"}""",
      s"""{"https://daml.com/ledger-api": {"ledgerId": "${testId: String}", "applicationId": "test", "admin": true}}""",
    )
    JwtSigner.HMAC256
      .sign(decodedJwt, "secret")
      .fold(e => fail(s"cannot sign a JWT: ${e.shows}"), identity)
  }

  implicit val `AHS asys`: ActorSystem = ActorSystem(testId)
  implicit val `AHS mat`: Materializer = Materializer(`AHS asys`)
  implicit val `AHS aesf`: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool(testId)(`AHS asys`)
  import shapeless.tag
  import tag.@@ // used for subtyping to make `AHS ec` beat executionContext
  implicit val `AHS ec`: ExecutionContext @@ this.type = tag[this.type](`AHS asys`.dispatcher)

  protected def withHttpServiceAndClient[A](
      testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, LedgerClient) => Future[A]
  ): Future[A] =
    HttpServiceTestFixture.withLedger[A](List(dar1, dar2), testId, None, useTls) {
      case (ledgerPort, _) =>
        HttpServiceTestFixture.withHttpService[A](
          testId,
          ledgerPort,
          jdbcConfig,
          staticContentConfig,
          useTls = useTls,
          wsConfig = wsConfig,
        )(testFn)
    }

  protected def withHttpService[A](
      f: (Uri, DomainJsonEncoder, DomainJsonDecoder) => Future[A]
  ): Future[A] =
    withHttpServiceAndClient((a, b, c, _) => f(a, b, c))

  protected def withLedger[A](testFn: LedgerClient => Future[A]): Future[A] =
    HttpServiceTestFixture.withLedger[A](List(dar1, dar2), testId) { case (_, client) =>
      testFn(client)
    }

  protected val headersWithAuth = authorizationHeader(jwt)

  protected def headersWithPartyAuth(actAs: List[String], readAs: List[String] = List()) =
    HttpServiceTestFixture.headersWithPartyAuth(actAs, readAs, testId)

  protected def postJsonStringRequest(
      uri: Uri,
      jsonString: String,
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.postJsonStringRequest(uri, jsonString, headersWithAuth)

  protected def postJsonRequest(
      uri: Uri,
      json: JsValue,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.postJsonRequest(uri, json, headers)

  protected def getRequest(
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.getRequest(uri, headers)

  protected def getResponseDataBytes(resp: HttpResponse, debug: Boolean = false): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
  }

  protected def postCreateCommand(
      cmd: domain.CreateCommand[v.Record],
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.postCreateCommand(cmd, encoder, uri, headers)

  protected def postArchiveCommand(
      templateId: domain.TemplateId.OptionalPkg,
      contractId: domain.ContractId,
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.postArchiveCommand(templateId, contractId, encoder, uri, headers)

  protected def lookupContractAndAssert(contractLocator: domain.ContractLocator[JsValue])(
      contractId: ContractId,
      create: domain.CreateCommand[v.Record],
      encoder: DomainJsonEncoder,
      uri: Uri,
  ): Future[Assertion] =
    postContractsLookup(contractLocator, uri).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val result = getResult(output)
      contractId shouldBe getContractId(result)
      assertActiveContract(result)(create, encoder)
    }

  protected def removeRecordId(a: v.Value): v.Value = a match {
    case v.Value(v.Value.Sum.Record(r)) if r.recordId.isDefined =>
      v.Value(v.Value.Sum.Record(removeRecordId(r)))
    case _ =>
      a
  }

  protected def removeRecordId(a: v.Record): v.Record = a.copy(recordId = None)

  protected def iouCreateCommand(
      amount: String = "999.9900000000",
      currency: String = "USD",
  ): domain.CreateCommand[v.Record] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val arg = v.Record(
      fields = List(
        v.RecordField("issuer", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("currency", Some(v.Value(v.Value.Sum.Text(currency)))),
        v.RecordField("amount", Some(v.Value(v.Value.Sum.Numeric(amount)))),
        v.RecordField("observers", Some(v.Value(v.Value.Sum.List(v.List())))),
      )
    )

    domain.CreateCommand(templateId, arg, None)
  }

  protected def iouExerciseTransferCommand(
      contractId: lar.ContractId
  ): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {
    val templateId = domain.TemplateId(None, "Iou", "Iou")
    val reference = domain.EnrichedContractId(Some(templateId), contractId)
    val arg =
      v.Record(fields = List(v.RecordField("newOwner", Some(v.Value(v.Value.Sum.Party("Bob"))))))
    val choice = lar.Choice("Iou_Transfer")

    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
  }

  protected def iouCreateAndExerciseTransferCommand(
      amount: String = "999.9900000000",
      currency: String = "USD",
  ): domain.CreateAndExerciseCommand[v.Record, v.Value] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val payload = v.Record(
      fields = List(
        v.RecordField("issuer", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("owner", Some(v.Value(v.Value.Sum.Party("Alice")))),
        v.RecordField("currency", Some(v.Value(v.Value.Sum.Text(currency)))),
        v.RecordField("amount", Some(v.Value(v.Value.Sum.Numeric(amount)))),
        v.RecordField("observers", Some(v.Value(v.Value.Sum.List(v.List())))),
      )
    )

    val arg =
      v.Record(fields = List(v.RecordField("newOwner", Some(v.Value(v.Value.Sum.Party("Bob"))))))
    val choice = lar.Choice("Iou_Transfer")

    domain.CreateAndExerciseCommand(
      templateId = templateId,
      payload = payload,
      choice = choice,
      argument = boxedRecord(arg),
      meta = None,
    )
  }

  protected def multiPartyCreateCommand(ps: List[String], value: String) = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Test", "MultiPartyContract")
    val psv = v.Value(v.Value.Sum.List(v.List(ps.map(p => v.Value(v.Value.Sum.Party(p))))))
    val payload = v.Record(
      fields = List(
        v.RecordField("parties", Some(psv)),
        v.RecordField("value", Some(v.Value(v.Value.Sum.Text(value)))),
      )
    )
    domain.CreateCommand(
      templateId = templateId,
      payload = payload,
      meta = None,
    )
  }

  protected def multiPartyAddSignatories(cid: lar.ContractId, ps: List[String]) = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Test", "MultiPartyContract")
    val psv = v.Value(v.Value.Sum.List(v.List(ps.map(p => v.Value(v.Value.Sum.Party(p))))))
    val argument = v.Value(
      v.Value.Sum.Record(
        v.Record(
          fields = List(v.RecordField("newParties", Some(psv)))
        )
      )
    )
    domain.ExerciseCommand(
      reference = domain.EnrichedContractId(Some(templateId), cid),
      argument = argument,
      choice = lar.Choice("MPAddSignatories"),
      meta = None,
    )
  }

  protected def multiPartyFetchOther(
      cid: lar.ContractId,
      fetchedCid: lar.ContractId,
      actors: List[String],
  ) = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Test", "MultiPartyContract")
    val argument = v.Value(
      v.Value.Sum.Record(
        v.Record(
          fields = List(
            v.RecordField("cid", Some(v.Value(v.Value.Sum.ContractId(fetchedCid.unwrap)))),
            v.RecordField(
              "actors",
              Some(
                v.Value(v.Value.Sum.List(v.List(actors.map(p => v.Value(v.Value.Sum.Party(p))))))
              ),
            ),
          )
        )
      )
    )
    domain.ExerciseCommand(
      reference = domain.EnrichedContractId(Some(templateId), cid),
      argument = argument,
      choice = lar.Choice("MPFetchOther"),
      meta = None,
    )
  }

  protected def assertStatus(jsObj: JsValue, expectedStatus: StatusCode): Assertion = {
    inside(jsObj) { case JsObject(fields) =>
      inside(fields.get("status")) { case Some(JsNumber(status)) =>
        status shouldBe BigDecimal(expectedStatus.intValue)
      }
    }
  }

  protected def expectedOneErrorMessage(output: JsValue): String =
    inside(output) { case JsObject(fields) =>
      inside(fields.get("errors")) { case Some(JsArray(Vector(JsString(errorMsg)))) =>
        errorMsg
      }
    }

  protected def postContractsLookup(
      cmd: domain.ContractLocator[JsValue],
      uri: Uri,
  ): Future[(StatusCode, JsValue)] =
    for {
      json <- toFuture(SprayJson.encode(cmd)): Future[JsValue]
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/fetch")), json)
    } yield result

  protected def activeContractList(output: JsValue): List[domain.ActiveContract[JsValue]] = {
    val result = getResult(output)
    SprayJson
      .decode[List[domain.ActiveContract[JsValue]]](result)
      .valueOr(e => fail(e.shows))
  }

  protected def activeContract(output: JsValue): domain.ActiveContract[JsValue] = {
    val result = getResult(output)
    SprayJson
      .decode[domain.ActiveContract[JsValue]](result)
      .valueOr(e => fail(e.shows))
  }

  protected def asContractId(a: JsValue): domain.ContractId = inside(a) { case JsString(x) =>
    domain.ContractId(x)
  }

  protected def encodeExercise(encoder: DomainJsonEncoder)(
      exercise: domain.ExerciseCommand[v.Value, domain.ContractLocator[v.Value]]
  ): JsValue =
    encoder.encodeExerciseCommand(exercise).getOrElse(fail(s"Cannot encode: $exercise"))

  protected def decodeExercise(
      decoder: DomainJsonDecoder
  )(jsVal: JsValue): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {

    import scalaz.syntax.bifunctor._

    val cmd = decoder.decodeExerciseCommand(jsVal).getOrElse(fail(s"Cannot decode $jsVal"))
    cmd.bimap(
      lfToApi,
      enrichedContractIdOnly,
    )
  }

  protected def enrichedContractIdOnly(x: domain.ContractLocator[_]): domain.EnrichedContractId =
    x match {
      case a: domain.EnrichedContractId => a
      case _: domain.EnrichedContractKey[_] =>
        fail(s"Expected domain.EnrichedContractId, got: $x")
    }

  protected def lfToApi(lfVal: domain.LfValue): v.Value =
    LfEngineToApi.lfValueToApiValue(verbose = true, lfVal).fold(e => fail(e), identity)

  protected def assertActiveContract(
      decoder: DomainJsonDecoder,
      actual: domain.ActiveContract[JsValue],
      create: domain.CreateCommand[v.Record],
      exercise: domain.ExerciseCommand[v.Value, _],
  ): Assertion = {

    val expectedContractFields: Seq[v.RecordField] = create.payload.fields
    val expectedNewOwner: v.Value = exercise.argument.sum.record
      .flatMap(_.fields.headOption)
      .flatMap(_.value)
      .getOrElse(fail("Cannot extract expected newOwner"))

    val active: domain.ActiveContract[v.Value] =
      decoder.decodeUnderlyingValues(actual).valueOr(e => fail(e.shows))

    inside(active.payload.sum.record.map(_.fields)) {
      case Some(
            Seq(
              v.RecordField("iou", Some(contractRecord)),
              v.RecordField("newOwner", Some(newOwner)),
            )
          ) =>
        val contractFields: Seq[v.RecordField] =
          contractRecord.sum.record.map(_.fields).getOrElse(Seq.empty)
        (contractFields: Seq[v.RecordField]) shouldBe (expectedContractFields: Seq[v.RecordField])
        (newOwner: v.Value) shouldBe (expectedNewOwner: v.Value)
    }
  }

  protected def assertActiveContract(
      jsVal: JsValue
  )(command: domain.CreateCommand[v.Record], encoder: DomainJsonEncoder): Assertion = {

    import encoder.implicits._

    val expected: domain.CreateCommand[JsValue] =
      command
        .traverse(SprayJson.encode[v.Record])
        .getOrElse(fail(s"Failed to encode command: $command"))

    inside(SprayJson.decode[domain.ActiveContract[JsValue]](jsVal)) { case \/-(activeContract) =>
      (activeContract.payload: JsValue) shouldBe (expected.payload: JsValue)
    }
  }

  protected def assertTemplateId(
      actual: domain.TemplateId.RequiredPkg,
      expected: domain.TemplateId.OptionalPkg,
  ): Assertion = {
    expected.packageId.foreach(x => actual.packageId shouldBe x)
    actual.moduleName shouldBe expected.moduleName
    actual.entityName shouldBe expected.entityName
  }

  protected def getAllPackageIds(uri: Uri): Future[domain.OkResponse[List[String]]] =
    getRequest(uri = uri.withPath(Uri.Path("/v1/packages"))).map { case (status, output) =>
      status shouldBe StatusCodes.OK
      inside(decode1[domain.OkResponse, List[String]](output)) { case \/-(x) =>
        x
      }
    }

  protected def initialIouCreate(serviceUri: Uri): Future[(StatusCode, JsValue)] = {
    val payload = TestUtil.readFile("it/iouCreateCommand.json")
    HttpServiceTestFixture.postJsonStringRequest(
      serviceUri.withPath(Uri.Path("/v1/create")),
      payload,
      headersWithAuth,
    )
  }

  protected def initialAccountCreate(
      serviceUri: Uri,
      encoder: DomainJsonEncoder,
  ): Future[(StatusCode, JsValue)] = {
    val command = accountCreateCommand(domain.Party("Alice"), "abc123")
    postCreateCommand(command, encoder, serviceUri)
  }

  protected def jsObject(s: String): JsObject = {
    val r: JsonError \/ JsObject = for {
      jsVal <- SprayJson.parse(s).leftMap(e => JsonError(e.shows))
      jsObj <- SprayJson.mustBeJsObject(jsVal)
    } yield jsObj
    r.valueOr(e => fail(e.shows))
  }

  protected def searchExpectOk(
      commands: List[domain.CreateCommand[v.Record]],
      query: JsObject,
      uri: Uri,
      encoder: DomainJsonEncoder,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[List[domain.ActiveContract[JsValue]]] = {
    search(commands, query, uri, encoder, headers).map(expectOk(_))
  }

  protected def search(
      commands: List[domain.CreateCommand[v.Record]],
      query: JsObject,
      uri: Uri,
      encoder: DomainJsonEncoder,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[
    domain.SyncResponse[List[domain.ActiveContract[JsValue]]]
  ] = {
    commands.traverse(c => postCreateCommand(c, encoder, uri, headers)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(commands.size)(StatusCodes.OK)
      postJsonRequest(uri.withPath(Uri.Path("/v1/query")), query, headers).flatMap {
        case (_, output) =>
          FutureUtil
            .toFuture(decode1[domain.SyncResponse, List[domain.ActiveContract[JsValue]]](output))
      }
    }
  }

  private[http] def expectOk[R](resp: domain.SyncResponse[R]): R = resp match {
    case ok: domain.OkResponse[_] =>
      ok.status shouldBe StatusCodes.OK
      ok.warnings shouldBe empty
      ok.result
    case err: domain.ErrorResponse =>
      fail(s"Expected OK response, got: $err")
  }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
abstract class AbstractHttpServiceIntegrationTest
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with AbstractHttpServiceIntegrationTestFuns {

  import json.JsonProtocol._
  import HttpServiceTestFixture._

  override final def useTls = UseTls.NoTls

  "query GET empty results" in withHttpService { (uri: Uri, _, _) =>
    searchAllExpectOk(uri).flatMap { case vector =>
      vector should have size 0L
    }

  }

  protected val searchDataSet: List[domain.CreateCommand[v.Record]] = List(
    iouCreateCommand(amount = "111.11", currency = "EUR"),
    iouCreateCommand(amount = "222.22", currency = "EUR"),
    iouCreateCommand(amount = "333.33", currency = "GBP"),
    iouCreateCommand(amount = "444.44", currency = "BTC"),
  )

  "query GET" in withHttpService { (uri: Uri, encoder, _) =>
    searchDataSet.traverse(c => postCreateCommand(c, encoder, uri)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

      getRequest(uri = uri.withPath(Uri.Path("/v1/query")))
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          inside(output) { case JsObject(fields) =>
            inside(fields.get("result")) { case Some(JsArray(vector)) =>
              vector should have size searchDataSet.size.toLong
            }
          }
        }: Future[Assertion]
    }
  }

  "multi-party query GET" in withHttpService { (uri, encoder, _) =>
    for {
      _ <- postCreateCommand(
        accountCreateCommand(owner = domain.Party("Alice"), number = "42"),
        encoder,
        uri,
      ).map(r => r._1 shouldBe StatusCodes.OK)
      _ <- postCreateCommand(
        accountCreateCommand(owner = domain.Party("Bob"), number = "23"),
        encoder,
        uri,
        headers = headersWithPartyAuth(List("Bob")),
      ).map(r => r._1 shouldBe StatusCodes.OK)
      _ <- searchAllExpectOk(uri, headersWithPartyAuth(List("Alice"))).map(cs =>
        cs should have size 1
      )
      _ <- searchAllExpectOk(uri, headersWithPartyAuth(List("Bob"))).map(cs =>
        cs should have size 1
      )
      _ <- searchAllExpectOk(uri, headersWithPartyAuth(List("Alice", "Bob"))).map(cs =>
        cs should have size 2
      )
    } yield succeed
  }

  "query POST with empty query" in withHttpService { (uri, encoder, _) =>
    searchExpectOk(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"]}"""),
      uri,
      encoder,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe searchDataSet.size
    }
  }

  "multi-party query POST with empty query" in withHttpService { (uri, encoder, _) =>
    for {
      aliceAccountResp <- postCreateCommand(
        accountCreateCommand(owner = domain.Party("Alice"), number = "42"),
        encoder,
        uri,
      )
      _ = aliceAccountResp._1 shouldBe StatusCodes.OK
      bobAccountResp <- postCreateCommand(
        accountCreateCommand(owner = domain.Party("Bob"), number = "23"),
        encoder,
        uri,
        headers = headersWithPartyAuth(List("Bob")),
      )
      _ = bobAccountResp._1 shouldBe StatusCodes.OK
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        headers = headersWithPartyAuth(List("Alice")),
      )
        .map(acl => acl.size shouldBe 1)
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        headers = headersWithPartyAuth(List("Bob")),
      )
        .map(acl => acl.size shouldBe 1)
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        headers = headersWithPartyAuth(List("Alice", "Bob")),
      )
        .map(acl => acl.size shouldBe 2)
    } yield {
      assert(true)
    }
  }

  "query with query, one field" in withHttpService { (uri, encoder, _) =>
    searchExpectOk(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
      uri,
      encoder,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 2
      acl.map(a => objectField(a.payload, "currency")) shouldBe List.fill(2)(Some(JsString("EUR")))
    }
  }

  "query returns unknown Template IDs as warnings" in withHttpService { (uri, encoder, _) =>
    val query =
      jsObject(
        """{"templateIds": ["Iou:Iou", "UnknownModule:UnknownEntity"], "query": {"currency": "EUR"}}"""
      )

    search(List(), query, uri, encoder).map { response =>
      inside(response) { case domain.OkResponse(acl, warnings, StatusCodes.OK) =>
        acl.size shouldBe 0
        warnings shouldBe Some(
          domain.UnknownTemplateIds(List(domain.TemplateId(None, "UnknownModule", "UnknownEntity")))
        )
      }
    }
  }

  "query returns unknown Template IDs as warnings and error" in withHttpService {
    (uri, encoder, _) =>
      search(
        searchDataSet,
        jsObject("""{"templateIds": ["AAA:BBB", "XXX:YYY"]}"""),
        uri,
        encoder,
      ).map { response =>
        inside(response) { case domain.ErrorResponse(errors, warnings, StatusCodes.BadRequest) =>
          errors shouldBe List(ErrorMessages.cannotResolveAnyTemplateId)
          inside(warnings) { case Some(domain.UnknownTemplateIds(unknownTemplateIds)) =>
            unknownTemplateIds.toSet shouldBe Set(
              domain.TemplateId(None, "AAA", "BBB"),
              domain.TemplateId(None, "XXX", "YYY"),
            )
          }
        }
      }
  }

  "query with query, can use number or string for numeric field" in withHttpService {
    (uri, encoder, _) =>
      import scalaz.std.scalaFuture._

      searchDataSet.traverse(c => postCreateCommand(c, encoder, uri)).flatMap {
        rs: List[(StatusCode, JsValue)] =>
          rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

          def queryAmountAs(s: String) =
            jsObject(s"""{"templateIds": ["Iou:Iou"], "query": {"amount": $s}}""")

          val queryAmountAsString = queryAmountAs("\"111.11\"")
          val queryAmountAsNumber = queryAmountAs("111.11")

          List(
            postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsString),
            postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsNumber),
          ).sequence.flatMap { rs: List[(StatusCode, JsValue)] =>
            rs.map(_._1) shouldBe List.fill(2)(StatusCodes.OK)
            inside(rs.map(_._2)) { case List(jsVal1, jsVal2) =>
              jsVal1 shouldBe jsVal2
              val acl1: List[domain.ActiveContract[JsValue]] = activeContractList(jsVal1)
              val acl2: List[domain.ActiveContract[JsValue]] = activeContractList(jsVal2)
              acl1 shouldBe acl2
              inside(acl1) { case List(ac) =>
                objectField(ac.payload, "amount") shouldBe Some(JsString("111.11"))
              }
            }
          }
      }: Future[Assertion]
  }

  "query with query, two fields" in withHttpService { (uri, encoder, _) =>
    searchExpectOk(
      searchDataSet,
      jsObject(
        """{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR", "amount": "111.11"}}"""
      ),
      uri,
      encoder,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 1
      acl.map(a => objectField(a.payload, "currency")) shouldBe List(Some(JsString("EUR")))
      acl.map(a => objectField(a.payload, "amount")) shouldBe List(Some(JsString("111.11")))
    }
  }

  "query with query, no results" in withHttpService { (uri, encoder, _) =>
    searchExpectOk(
      searchDataSet,
      jsObject(
        """{"templateIds": ["Iou:Iou"], "query": {"currency": "RUB", "amount": "666.66"}}"""
      ),
      uri,
      encoder,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 0
    }
  }

  "query with invalid JSON query should return error" in withHttpService { (uri, _, _) =>
    postJsonStringRequest(uri.withPath(Uri.Path("/v1/query")), "{NOT A VALID JSON OBJECT")
      .flatMap { case (status, output) =>
        status shouldBe StatusCodes.BadRequest
        assertStatus(output, StatusCodes.BadRequest)
      }: Future[Assertion]
  }

  protected def searchAllExpectOk(
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[List[domain.ActiveContract[JsValue]]] =
    searchAll(uri, headers).map(expectOk(_))

  protected def searchAll(
      uri: Uri,
      headers: List[HttpHeader],
  ): Future[domain.SyncResponse[List[domain.ActiveContract[JsValue]]]] = {
    getRequest(uri = uri.withPath(Uri.Path("/v1/query")), headers)
      .flatMap { case (_, output) =>
        FutureUtil.toFuture(
          decode1[domain.SyncResponse, List[domain.ActiveContract[JsValue]]](output)
        )
      }
  }

  "create IOU" in withHttpService { (uri, encoder, _) =>
    val command: domain.CreateCommand[v.Record] = iouCreateCommand()

    postCreateCommand(command, encoder, uri).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val activeContract = getResult(output)
      assertActiveContract(activeContract)(command, encoder)
    }: Future[Assertion]
  }

  "create IOU should fail if authorization header is missing" in withHttpService {
    (uri, encoder, _) =>
      import encoder.implicits._

      val command: domain.CreateCommand[v.Record] = iouCreateCommand()
      val input: JsValue = SprayJson.encode1(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input, List()).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          expectedOneErrorMessage(output) should include(
            "missing Authorization header with OAuth 2.0 Bearer Token"
          )
      }: Future[Assertion]
  }

  "create IOU should support extra readAs parties" in withHttpService { (uri, encoder, _) =>
    import encoder.implicits._

    val command: domain.CreateCommand[v.Record] = iouCreateCommand()
    val input: JsValue = SprayJson.encode1(command).valueOr(e => fail(e.shows))

    postJsonRequest(
      uri.withPath(Uri.Path("/v1/create")),
      input,
      headers = headersWithPartyAuth(actAs = List("Alice"), readAs = List("Bob")),
    ).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val activeContract = getResult(output)
      assertActiveContract(activeContract)(command, encoder)
    }: Future[Assertion]
  }

  "create IOU with unsupported templateId should return proper error" in withHttpService {
    (uri, encoder, _) =>
      import encoder.implicits._

      val command: domain.CreateCommand[v.Record] =
        iouCreateCommand().copy(templateId = domain.TemplateId(None, "Iou", "Dummy"))
      val input: JsValue = SprayJson.encode1(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
          val unknownTemplateId: domain.TemplateId.OptionalPkg =
            domain.TemplateId(None, command.templateId.moduleName, command.templateId.entityName)
          expectedOneErrorMessage(output) should include(
            s"Cannot resolve template ID, given: ${unknownTemplateId: domain.TemplateId.OptionalPkg}"
          )
      }: Future[Assertion]
  }

  "exercise IOU_Transfer" in withHttpService { (uri, encoder, decoder) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap { case (createStatus, createOutput) =>
        createStatus shouldBe StatusCodes.OK
        assertStatus(createOutput, StatusCodes.OK)

        val contractId = getContractId(getResult(createOutput))
        val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
          iouExerciseTransferCommand(contractId)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
          .flatMap { case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
            assertExerciseResponseNewActiveContract(
              getResult(exerciseOutput),
              create,
              exercise,
              decoder,
              uri,
            )
          }
      }: Future[Assertion]
  }

  "create-and-exercise IOU_Transfer" in withHttpService { (uri, encoder, _) =>
    import encoder.implicits._

    val cmd: domain.CreateAndExerciseCommand[v.Record, v.Value] =
      iouCreateAndExerciseTransferCommand()

    val json: JsValue = SprayJson.encode2(cmd).valueOr(e => fail(e.shows))

    postJsonRequest(uri.withPath(Uri.Path("/v1/create-and-exercise")), json)
      .flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        inside(
          decode1[domain.OkResponse, domain.ExerciseResponse[JsValue]](output)
        ) { case \/-(response) =>
          response.status shouldBe StatusCodes.OK
          response.warnings shouldBe empty
          inside(response.result.events) {
            case List(
                  domain.Contract(\/-(created0)),
                  domain.Contract(-\/(archived0)),
                  domain.Contract(\/-(created1)),
                ) =>
              assertTemplateId(created0.templateId, cmd.templateId)
              assertTemplateId(archived0.templateId, cmd.templateId)
              archived0.contractId shouldBe created0.contractId
              assertTemplateId(created1.templateId, domain.TemplateId(None, "Iou", "IouTransfer"))
              asContractId(response.result.exerciseResult) shouldBe created1.contractId
          }
        }
      }: Future[Assertion]
  }

  private def assertExerciseResponseNewActiveContract(
      exerciseResponse: JsValue,
      createCmd: domain.CreateCommand[v.Record],
      exerciseCmd: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
      decoder: DomainJsonDecoder,
      uri: Uri,
  ): Future[Assertion] = {
    inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](exerciseResponse)) {
      case \/-(domain.ExerciseResponse(JsString(exerciseResult), List(contract1, contract2))) => {
        // checking contracts
        inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
          (archivedContract.contractId.unwrap: String) shouldBe (exerciseCmd.reference.contractId.unwrap: String)
        }
        inside(contract2) { case domain.Contract(\/-(activeContract)) =>
          assertActiveContract(decoder, activeContract, createCmd, exerciseCmd)
        }
        // checking exerciseResult
        exerciseResult.length should be > (0)
        val newContractLocator = domain.EnrichedContractId(
          Some(domain.TemplateId(None, "Iou", "IouTransfer")),
          domain.ContractId(exerciseResult),
        )
        postContractsLookup(newContractLocator, uri).flatMap { case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getContractId(getResult(output)) shouldBe newContractLocator.contractId
        }: Future[Assertion]
      }
    }
  }

  "exercise IOU_Transfer with unknown contractId should return proper error" in withHttpService {
    (uri, encoder, _) =>
      val contractId = lar.ContractId("#NonExistentContractId")
      val exerciseJson: JsValue = encodeExercise(encoder)(iouExerciseTransferCommand(contractId))
      postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.InternalServerError
          assertStatus(output, StatusCodes.InternalServerError)
          expectedOneErrorMessage(output) should include(
            "Contract could not be found with id ContractId(#NonExistentContractId)"
          )
        }: Future[Assertion]
  }

  "exercise Archive" in withHttpService { (uri, encoder, _) =>
    val create: domain.CreateCommand[v.Record] = iouCreateCommand()
    postCreateCommand(create, encoder, uri)
      .flatMap { case (createStatus, createOutput) =>
        createStatus shouldBe StatusCodes.OK
        assertStatus(createOutput, StatusCodes.OK)

        val contractId = getContractId(getResult(createOutput))
        val templateId = domain.TemplateId(None, "Iou", "Iou")
        val reference = domain.EnrichedContractId(Some(templateId), contractId)
        val exercise = archiveCommand(reference)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
          .flatMap { case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
            val exercisedResponse: JsObject = getResult(exerciseOutput).asJsObject
            assertExerciseResponseArchivedContract(exercisedResponse, exercise)
          }
      }: Future[Assertion]
  }

  "should support multi-party command submissions" in withHttpService { (uri, encoder, _) =>
    val newDar = AbstractHttpServiceIntegrationTestFuns.dar3
    for {
      _ <- Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.POST,
            uri = uri.withPath(Uri.Path("/v1/packages")),
            headers = authorizationHeader(jwtAdminNoParty),
            entity = HttpEntity.fromFile(ContentTypes.`application/octet-stream`, newDar),
          )
        )
      // multi-party actAs on create
      cid <- postCreateCommand(
        multiPartyCreateCommand(List("Alice", "Bob"), ""),
        encoder,
        uri,
        headersWithPartyAuth(List("Alice", "Bob")),
      ).map { case (status, output) =>
        status shouldBe StatusCodes.OK
        getContractId(getResult(output))
      }
      // multi-party actAs on exercise
      cidMulti <- postJsonRequest(
        uri.withPath(Uri.Path("/v1/exercise")),
        encodeExercise(encoder)(multiPartyAddSignatories(cid, List("Charlie", "David"))),
        headersWithPartyAuth(List("Alice", "Bob", "Charlie", "David")),
      ).map { case (status, output) =>
        status shouldBe StatusCodes.OK
        inside(getChild(getResult(output), "exerciseResult")) { case JsString(c) =>
          lar.ContractId(c)
        }
      }
      // create a contract only visible to Alice
      cid <- postCreateCommand(
        multiPartyCreateCommand(List("Alice"), ""),
        encoder,
        uri,
        headersWithPartyAuth(List("Alice")),
      ).map { case (status, output) =>
        status shouldBe StatusCodes.OK
        getContractId(getResult(output))
      }
      _ <- postJsonRequest(
        uri.withPath(Uri.Path("/v1/exercise")),
        encodeExercise(encoder)(multiPartyFetchOther(cidMulti, cid, List("Charlie"))),
        headersWithPartyAuth(List("Charlie"), readAs = List("Alice")),
      ).map { case (status, _) =>
        status shouldBe StatusCodes.OK
      }
    } yield succeed
  }

  private def assertExerciseResponseArchivedContract(
      exerciseResponse: JsValue,
      exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
  ): Assertion = {
    inside(exerciseResponse) { case result @ JsObject(_) =>
      inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](result)) {
        case \/-(domain.ExerciseResponse(exerciseResult, List(contract1))) =>
          exerciseResult shouldBe JsObject()
          inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
            (archivedContract.contractId.unwrap: String) shouldBe (exercise.reference.contractId.unwrap: String)
          }
      }
    }
  }

  "should be able to serialize and deserialize domain commands" in withLedger { client =>
    jsonCodecs(client).map { case (encoder, decoder) =>
      testCreateCommandEncodingDecoding(encoder, decoder)
      testExerciseCommandEncodingDecoding(encoder, decoder)
    }: Future[Assertion]
  }

  private def testCreateCommandEncodingDecoding(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
  ): Assertion = {
    import encoder.implicits._
    import json.JsonProtocol._
    import util.ErrorOps._

    val command0: domain.CreateCommand[v.Record] = iouCreateCommand()

    val x = for {
      jsVal <- SprayJson.encode1(command0).liftErr(JsonError)
      command1 <- decoder.decodeCreateCommand(jsVal)
    } yield command1.map(removeRecordId) should ===(command0)

    x.fold(e => fail(e.shows), identity)
  }

  private def testExerciseCommandEncodingDecoding(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
  ): Assertion = {
    val command0 = iouExerciseTransferCommand(lar.ContractId("#a-contract-ID"))
    val jsVal: JsValue = encodeExercise(encoder)(command0)
    val command1 = decodeExercise(decoder)(jsVal)
    command1.bimap(removeRecordId, identity) should ===(command0)
  }

  "request non-existent endpoint should return 404 with errors" in withHttpService {
    (uri: Uri, _, _) =>
      val badUri = uri.withPath(Uri.Path("/contracts/does-not-exist"))
      getRequest(uri = badUri)
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.NotFound
          assertStatus(output, StatusCodes.NotFound)
          expectedOneErrorMessage(
            output
          ) shouldBe s"${HttpMethods.GET: HttpMethod}, uri: ${badUri: Uri}"
        }: Future[Assertion]
  }

  "parties endpoint should return all known parties" in withHttpServiceAndClient {
    (uri, _, _, client) =>
      import scalaz.std.vector._
      val partyIds = Vector("Alice", "Bob", "Charlie", "Dave")
      val partyManagement = client.partyManagementClient

      partyIds
        .traverse { p =>
          partyManagement.allocateParty(Some(p), Some(s"$p & Co. LLC"))
        }
        .flatMap { allocatedParties =>
          getRequest(uri = uri.withPath(Uri.Path("/v1/parties"))).flatMap { case (status, output) =>
            status shouldBe StatusCodes.OK
            inside(
              decode1[domain.OkResponse, List[domain.PartyDetails]](output)
            ) { case \/-(response) =>
              response.status shouldBe StatusCodes.OK
              response.warnings shouldBe empty
              val actualIds: Set[domain.Party] = response.result.view.map(_.identifier).toSet
              actualIds shouldBe domain.Party.subst(partyIds.toSet)
              response.result.toSet shouldBe
                allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
            }
          }
        }: Future[Assertion]
  }

  "parties endpoint should return only requested parties, unknown parties returned as warnings" in withHttpServiceAndClient {
    (uri, _, _, client) =>
      import scalaz.std.vector._

      val charlie = domain.Party("Charlie")
      val knownParties = domain.Party.subst(Vector("Alice", "Bob", "Dave")) :+ charlie
      val erin = domain.Party("Erin")
      val requestedPartyIds: Vector[domain.Party] = knownParties.filterNot(_ == charlie) :+ erin

      val partyManagement = client.partyManagementClient

      knownParties
        .traverse { p =>
          partyManagement.allocateParty(Some(p.unwrap), Some(s"${p.unwrap} & Co. LLC"))
        }
        .flatMap { allocatedParties =>
          postJsonRequest(
            uri = uri.withPath(Uri.Path("/v1/parties")),
            JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
          ).flatMap { case (status, output) =>
            status shouldBe StatusCodes.OK
            inside(
              decode1[domain.OkResponse, List[domain.PartyDetails]](output)
            ) { case \/-(response) =>
              response.status shouldBe StatusCodes.OK
              response.warnings shouldBe Some(domain.UnknownParties(List(erin)))
              val actualIds: Set[domain.Party] = response.result.view.map(_.identifier).toSet
              actualIds shouldBe requestedPartyIds.toSet - erin // Erin is not known
              val expected: Set[domain.PartyDetails] = allocatedParties.toSet
                .map(domain.PartyDetails.fromLedgerApi)
                .filterNot(_.identifier == charlie)
              response.result.toSet shouldBe expected
            }
          }
        }: Future[Assertion]
  }

  "parties endpoint should error if empty array passed as input" in withHttpServiceAndClient {
    (uri, _, _, _) =>
      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(Vector.empty),
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.BadRequest
        assertStatus(output, StatusCodes.BadRequest)
        val errorMsg = expectedOneErrorMessage(output)
        errorMsg should include("Cannot read JSON: <[]>")
        errorMsg should include("must be a JSON array with at least 1 element")
      }: Future[Assertion]
  }

  "parties endpoint returns error if empty party string passed" in withHttpServiceAndClient {
    (uri, _, _, _) =>
      val requestedPartyIds: Vector[domain.Party] = domain.Party.subst(Vector(""))

      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.BadRequest
        inside(decode1[domain.SyncResponse, List[domain.PartyDetails]](output)) {
          case \/-(domain.ErrorResponse(List(error), None, StatusCodes.BadRequest)) =>
            error should include("DAML LF Party is empty")
        }
      }: Future[Assertion]
  }

  "parties endpoint returns empty result with warnings and OK status if nothing found" in withHttpServiceAndClient {
    (uri, _, _, _) =>
      val requestedPartyIds: Vector[domain.Party] =
        domain.Party.subst(Vector("Alice", "Bob", "Dave"))

      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        inside(decode1[domain.SyncResponse, List[domain.PartyDetails]](output)) {
          case \/-(domain.OkResponse(List(), Some(warnings), StatusCodes.OK)) =>
            inside(warnings) { case domain.UnknownParties(unknownParties) =>
              unknownParties.toSet shouldBe requestedPartyIds.toSet
            }
        }
      }: Future[Assertion]
  }

  "parties/allocate should allocate a new party" in withHttpServiceAndClient { (uri, _, _, _) =>
    val request = domain.AllocatePartyRequest(
      Some(domain.Party(s"Carol${uniqueId()}")),
      Some("Carol & Co. LLC"),
    )
    val json = SprayJson.encode(request).valueOr(e => fail(e.shows))

    postJsonRequest(
      uri = uri.withPath(Uri.Path("/v1/parties/allocate")),
      json = json,
      headers = authorizationHeader(jwtAdminNoParty),
    )
      .flatMap { case (status, output) =>
        status shouldBe StatusCodes.OK
        inside(decode1[domain.OkResponse, domain.PartyDetails](output)) { case \/-(response) =>
          response.status shouldBe StatusCodes.OK
          val newParty = response.result
          Some(newParty.identifier) shouldBe request.identifierHint
          newParty.displayName shouldBe request.displayName
          newParty.isLocal shouldBe true

          getRequest(uri = uri.withPath(Uri.Path("/v1/parties"))).flatMap { case (status, output) =>
            status shouldBe StatusCodes.OK
            inside(decode1[domain.OkResponse, List[domain.PartyDetails]](output)) {
              case \/-(response) =>
                response.status shouldBe StatusCodes.OK
                response.result should contain(newParty)
            }
          }
        }
      }: Future[Assertion]
  }

  "parties/allocate should allocate a new party without any hints" in withHttpServiceAndClient {
    (uri, _, _, _) =>
      postJsonRequest(uri = uri.withPath(Uri.Path("/v1/parties/allocate")), json = JsObject())
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.OK
          inside(decode1[domain.OkResponse, domain.PartyDetails](output)) { case \/-(response) =>
            response.status shouldBe StatusCodes.OK
            val newParty = response.result
            newParty.identifier.unwrap.length should be > 0
            newParty.displayName shouldBe None
            newParty.isLocal shouldBe true

            getRequest(uri = uri.withPath(Uri.Path("/v1/parties"))).flatMap {
              case (status, output) =>
                status shouldBe StatusCodes.OK
                inside(decode1[domain.OkResponse, List[domain.PartyDetails]](output)) {
                  case \/-(response) =>
                    response.status shouldBe StatusCodes.OK
                    response.result should contain(newParty)
                }
            }
          }
        }: Future[Assertion]
  }

  "parties/allocate should return BadRequest error if party ID hint is invalid PartyIdString" in withHttpServiceAndClient {
    (uri, _, _, _) =>
      val request = domain.AllocatePartyRequest(
        Some(domain.Party(s"Carol-!")),
        Some("Carol & Co. LLC"),
      )
      val json = SprayJson.encode(request).valueOr(e => fail(e.shows))

      postJsonRequest(uri = uri.withPath(Uri.Path("/v1/parties/allocate")), json = json)
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          inside(decode[domain.ErrorResponse](output)) { case \/-(response) =>
            response.status shouldBe StatusCodes.BadRequest
            response.warnings shouldBe empty
            response.errors.length shouldBe 1
          }
        }
  }

  "fetch by contractId" in withHttpService { (uri, encoder, _) =>
    val command: domain.CreateCommand[v.Record] = iouCreateCommand()

    postCreateCommand(command, encoder, uri).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val contractId: ContractId = getContractId(getResult(output))
      val locator = domain.EnrichedContractId(None, contractId)
      lookupContractAndAssert(locator)(contractId, command, encoder, uri)
    }: Future[Assertion]
  }

  "fetch returns {status:200, result:null} when contract is not found" in withHttpService {
    (uri, _, _) =>
      val owner = domain.Party("Alice")
      val accountNumber = "abc123"
      val locator = domain.EnrichedContractKey(
        domain.TemplateId(None, "Account", "Account"),
        JsArray(JsString(owner.unwrap), JsString(accountNumber)),
      )
      postContractsLookup(locator, uri.withPath(Uri.Path("/v1/fetch"))).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          output
            .asJsObject(s"expected JsObject, got: $output")
            .fields
            .get("result") shouldBe Some(JsNull)
      }: Future[Assertion]
  }

  "fetch by key" in withHttpService { (uri, encoder, _) =>
    val owner = domain.Party("Alice")
    val accountNumber = "abc123"
    val command: domain.CreateCommand[v.Record] =
      accountCreateCommand(owner, accountNumber)

    postCreateCommand(command, encoder, uri).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val contractId: ContractId = getContractId(getResult(output))
      val locator = domain.EnrichedContractKey(
        domain.TemplateId(None, "Account", "Account"),
        JsArray(JsString(owner.unwrap), JsString(accountNumber)),
      )
      lookupContractAndAssert(locator)(contractId, command, encoder, uri)
    }: Future[Assertion]
  }

  "commands/exercise Archive by key" in withHttpService { (uri, encoder, _) =>
    val owner = domain.Party("Alice")
    val accountNumber = "abc123"
    val create: domain.CreateCommand[v.Record] =
      accountCreateCommand(owner, accountNumber)

    val keyRecord = v.Record(
      fields = Seq(
        v.RecordField(value = Some(v.Value(v.Value.Sum.Party(owner.unwrap)))),
        v.RecordField(value = Some(v.Value(v.Value.Sum.Text(accountNumber)))),
      )
    )
    val locator = domain.EnrichedContractKey[v.Value](
      domain.TemplateId(None, "Account", "Account"),
      v.Value(v.Value.Sum.Record(keyRecord)),
    )
    val archive: domain.ExerciseCommand[v.Value, domain.EnrichedContractKey[v.Value]] =
      archiveCommand(locator)
    val archiveJson: JsValue = encodeExercise(encoder)(archive)

    postCreateCommand(create, encoder, uri).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)

      postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), archiveJson).flatMap {
        case (exerciseStatus, exerciseOutput) =>
          exerciseStatus shouldBe StatusCodes.OK
          assertStatus(exerciseOutput, StatusCodes.OK)
      }
    }: Future[Assertion]
  }

  "fetch by key containing variant and record, encoded as array with number num" in withHttpService {
    (uri, _, _) =>
      testFetchByCompositeKey(
        uri,
        jsObject("""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": [
              "Alice",
              {"tag": "Bar", "value": 42},
              {"baz": "another baz value"}
            ]
          }"""),
      )
  }

  "fetch by key containing variant and record, encoded as record with string num" in withHttpService {
    (uri, _, _) =>
      testFetchByCompositeKey(
        uri,
        jsObject("""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": {
              "_1": "Alice",
              "_2": {"tag": "Bar", "value": "42"},
              "_3": {"baz": "another baz value"}
            }
          }"""),
      )
  }

  private def testFetchByCompositeKey(uri: Uri, request: JsObject) = {
    val createCommand = jsObject("""{
        "templateId": "Account:KeyedByVariantAndRecord",
        "payload": {
          "name": "ABC DEF",
          "party": "Alice",
          "age": 123,
          "fooVariant": {"tag": "Bar", "value": 42},
          "bazRecord": {"baz": "another baz value"}
        }
      }""")

    postJsonRequest(uri.withPath(Uri.Path("/v1/create")), createCommand).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))

        postJsonRequest(uri.withPath(Uri.Path("/v1/fetch")), request).flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            activeContract(output).contractId shouldBe contractId
        }
    }: Future[Assertion]
  }

  "query by a variant field" in withHttpService { (uri, encoder, _) =>
    val owner = domain.Party("Alice")
    val accountNumber = "abc123"
    val now = TimestampConversion.instantToMicros(Instant.now)
    val nowStr = TimestampConversion.microsToInstant(now).toString
    val command: domain.CreateCommand[v.Record] =
      accountCreateCommand(owner, accountNumber, now)

    val packageId: Ref.PackageId = MetadataReader
      .templateByName(metadata2)(Ref.QualifiedName.assertFromString("Account:Account"))
      .headOption
      .map(_._1)
      .getOrElse(fail(s"Cannot retrieve packageId"))

    postCreateCommand(command, encoder, uri).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val contractId: ContractId = getContractId(getResult(output))

      val query = jsObject(s"""{
             "templateIds": ["$packageId:Account:Account"],
             "query": {
                 "number" : "abc123",
                 "status" : {"tag": "Enabled", "value": "${nowStr: String}"}
             }
          }""")

      postJsonRequest(uri.withPath(Uri.Path("/v1/query")), query).map {
        case (searchStatus, searchOutput) =>
          searchStatus shouldBe StatusCodes.OK
          assertStatus(searchOutput, StatusCodes.OK)
          inside(activeContractList(searchOutput)) { case List(ac) =>
            ac.contractId shouldBe contractId
          }
      }
    }: Future[Assertion]
  }

  "packages endpoint should return all known package IDs" in withHttpServiceAndClient {
    (uri, _, _, _) =>
      getAllPackageIds(uri).map { x =>
        inside(x) {
          case domain.OkResponse(ps, None, StatusCodes.OK) if ps.nonEmpty =>
            Inspectors.forAll(ps)(_.length should be > 0)
        }
      }: Future[Assertion]
  }

  "packages/packageId should return a requested package" in withHttpServiceAndClient {
    import AbstractHttpServiceIntegrationTestFuns.sha256

    (uri, _, _, _) =>
      getAllPackageIds(uri).flatMap { okResp =>
        inside(okResp.result.headOption) { case Some(packageId) =>
          Http()
            .singleRequest(
              HttpRequest(
                method = HttpMethods.GET,
                uri = uri.withPath(Uri.Path(s"/v1/packages/$packageId")),
                headers = authorizationHeader(jwtAdminNoParty),
              )
            )
            .map { resp =>
              resp.status shouldBe StatusCodes.OK
              resp.entity.getContentType() shouldBe ContentTypes.`application/octet-stream`
              sha256(resp.entity.dataBytes) shouldBe Success(packageId)
            }
        }
      }: Future[Assertion]
  }

  "packages upload endpoint" in withHttpServiceAndClient { (uri, _, _, _) =>
    val newDar = AbstractHttpServiceIntegrationTestFuns.dar3

    getAllPackageIds(uri).flatMap { okResp =>
      val existingPackageIds: Set[String] = okResp.result.toSet
      Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.POST,
            uri = uri.withPath(Uri.Path("/v1/packages")),
            headers = authorizationHeader(jwtAdminNoParty),
            entity = HttpEntity.fromFile(ContentTypes.`application/octet-stream`, newDar),
          )
        )
        .flatMap { resp =>
          resp.status shouldBe StatusCodes.OK
          getAllPackageIds(uri).map { okResp =>
            val newPackageIds: Set[String] = okResp.result.toSet -- existingPackageIds
            newPackageIds.size should be > 0
          }
        }
    }: Future[Assertion]
  }
}
