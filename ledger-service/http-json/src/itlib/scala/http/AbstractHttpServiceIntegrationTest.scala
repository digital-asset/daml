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
import com.daml.http.dbbackend.JdbcConfig
import com.daml.http.domain.ContractId
import com.daml.http.domain.TemplateId.OptionalPkg
import com.daml.http.json.SprayJson.{decode, decode1, objectField}
import com.daml.http.json._
import com.daml.http.util.ClientUtil.{boxedRecord, uniqueId}
import com.daml.http.util.FutureUtil.toFuture
import com.daml.http.util.{FutureUtil, SandboxTestLedger}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.{DecodedJwt, Jwt}
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.v1.{value => v}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.service.MetadataReader
import com.daml.ledger.test.{ModelTestDar, SemanticTestDar}
import com.daml.platform.participant.util.LfEngineToApi.lfValueToApiValue
import com.daml.http.util.Logging.instanceUUIDLogCtx
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
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
import scalaz.syntax.std.option._
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json._
import scalaz.syntax.apply._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.ports.Port

object AbstractHttpServiceIntegrationTestFuns {
  private[http] val dar1 = requiredResource(ModelTestDar.path)

  private[http] val dar2 = requiredResource("ledger-service/http-json/Account.dar")

  private[http] val dar3 = requiredResource(SemanticTestDar.path)

  private[http] val userDar = requiredResource("ledger-service/http-json/User.dar")

  def sha256(source: Source[ByteString, Any])(implicit mat: Materializer): Try[String] = Try {
    import java.security.MessageDigest
    import com.google.common.io.BaseEncoding

    val md = MessageDigest.getInstance("SHA-256")
    val is = source.runWith(StreamConverters.asInputStream())
    val dis = new DigestInputStream(is, md)

    // drain the input stream and calculate the hash
    while (-1 != dis.read()) ()

    dis.on(false)

    BaseEncoding.base16().lowerCase().encode(md.digest())
  }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait AbstractHttpServiceIntegrationTestFuns
    extends StrictLogging
    with SandboxTestLedger
    with SuiteResourceManagementAroundAll {
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

  protected val metadataUser: MetadataReader.LfMetadata =
    MetadataReader.readFromDar(userDar).valueOr(e => fail(s"Cannot read userDar metadata: $e"))

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

  import com.typesafe.config.ConfigFactory
  private val customConf = ConfigFactory.parseString("""
    akka.http.server.request-timeout = 60s
  """)

  implicit val `AHS asys`: ActorSystem = ActorSystem(testId, ConfigFactory.load(customConf))
  implicit val `AHS mat`: Materializer = Materializer(`AHS asys`)
  implicit val `AHS aesf`: ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool(testId)(`AHS asys`)
  import shapeless.tag
  import tag.@@ // used for subtyping to make `AHS ec` beat executionContext
  implicit val `AHS ec`: ExecutionContext @@ this.type = tag[this.type](`AHS asys`.dispatcher)

  override def packageFiles = List(dar1, dar2, userDar)

  protected def getUniqueParty(name: String) = getUniquePartyAndAuthHeaders(name)._1
  protected def getUniquePartyAndAuthHeaders(name: String): (domain.Party, List[HttpHeader]) = {
    val partyName = s"${name}_${uniqueId()}"
    (domain.Party(partyName), headersWithPartyAuth(List(partyName)))
  }

  protected def withHttpServiceAndClient[A](
      testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, DamlLedgerClient, LedgerId) => Future[A]
  ): Future[A] = usingLedger[A](testId) { case (ledgerPort, _, ledgerId) =>
    HttpServiceTestFixture.withHttpService[A](
      testId,
      ledgerPort,
      jdbcConfig,
      staticContentConfig,
      useTls = useTls,
      wsConfig = wsConfig,
    )(testFn(_, _, _, _, ledgerId))
  }

  protected def withHttpServiceAndClient[A](maxInboundMessageSize: Int)(
      testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, DamlLedgerClient, LedgerId) => Future[A]
  ): Future[A] = usingLedger[A](testId) { case (ledgerPort, _, ledgerId) =>
    HttpServiceTestFixture.withHttpService[A](
      testId,
      ledgerPort,
      jdbcConfig,
      staticContentConfig,
      useTls = useTls,
      wsConfig = wsConfig,
      maxInboundMessageSize = maxInboundMessageSize,
    )(testFn(_, _, _, _, ledgerId))
  }

  protected def withHttpServiceAndClient[A](token: Jwt)(
      testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, DamlLedgerClient, LedgerId) => Future[A]
  ): Future[A] = usingLedger[A](testId, Some(token.value)) { case (ledgerPort, _, ledgerId) =>
    HttpServiceTestFixture.withHttpService[A](
      testId,
      ledgerPort,
      jdbcConfig,
      staticContentConfig,
      useTls = useTls,
      wsConfig = wsConfig,
      token = Some(token),
    )(testFn(_, _, _, _, ledgerId))
  }

  protected def withHttpService[A](
      f: (Uri, DomainJsonEncoder, DomainJsonDecoder, LedgerId) => Future[A]
  ): Future[A] =
    withHttpServiceAndClient((a, b, c, _, ledgerId) => f(a, b, c, ledgerId))

  protected def withHttpServiceOnly[A](ledgerPort: Port)(
      f: (Uri, DomainJsonEncoder, DomainJsonDecoder) => Future[A]
  ): Future[A] =
    HttpServiceTestFixture.withHttpService[A](
      testId,
      ledgerPort,
      jdbcConfig,
      staticContentConfig,
      useTls = useTls,
      wsConfig = wsConfig,
    )((uri, encoder, decoder, _) => f(uri, encoder, decoder))

  protected def withLedger[A](testFn: (DamlLedgerClient, LedgerId) => Future[A]): Future[A] =
    usingLedger[A](testId) { case (_, client, ledgerId) =>
      testFn(client, ledgerId)
    }

  protected def withLedger2[A](testFn: (Port, DamlLedgerClient, LedgerId) => Future[A]): Future[A] =
    usingLedger[A](testId)(testFn)

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
      cmd: domain.CreateCommand[v.Record, OptionalPkg],
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.postCreateCommand(cmd, encoder, uri, headers)

  protected def postArchiveCommand(
      templateId: OptionalPkg,
      contractId: domain.ContractId,
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.postArchiveCommand(templateId, contractId, encoder, uri, headers)

  protected def lookupContractAndAssert(contractLocator: domain.ContractLocator[JsValue])(
      contractId: ContractId,
      create: domain.CreateCommand[v.Record, OptionalPkg],
      encoder: DomainJsonEncoder,
      uri: Uri,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[Assertion] =
    postContractsLookup(contractLocator, uri, headers).flatMap { case (status, output) =>
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

  protected def removePackageId(tmplId: domain.TemplateId.RequiredPkg): OptionalPkg =
    tmplId.copy(packageId = None)

  import com.daml.lf.data.{Numeric => LfNumeric}
  import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
  import shapeless.HList, shapeless.record.{Record => ShRecord}

  private[this] object RecordFromFields extends shapeless.Poly1 {
    import shapeless.Witness, shapeless.labelled.{FieldType => :->>:}
    implicit def elem[V, K <: Symbol](implicit
        fn: Witness.Aux[K]
    ): Case.Aux[K :->>: V, (String, V)] =
      at[K :->>: V]((fn.value.name, _))
  }

  protected[this] def recordFromFields[L <: HList, I <: HList](hlist: L)(implicit
      mapper: shapeless.ops.hlist.Mapper.Aux[RecordFromFields.type, L, I],
      lister: shapeless.ops.hlist.ToTraversable.Aux[I, Seq, (String, v.Value.Sum)],
  ): v.Record = v.Record(fields = hlist.map(RecordFromFields).to[Seq].map { case (n, vs) =>
    v.RecordField(n, Some(v.Value(vs)))
  })

  private[this] def argToApi(va: VA)(arg: va.Inj): v.Record =
    lfToApi(va.inj(arg)) match {
      case v.Value(v.Value.Sum.Record(r)) => removeRecordId(r)
      case _ => fail(s"${va.t} isn't a record type")
    }

  private[this] val (_, iouVA) = {
    import com.daml.lf.value.test.TypedValueGenerators.RNil
    import shapeless.syntax.singleton._, com.daml.lf.data.Numeric.Scale
    val iouT = Symbol("issuer") ->> VA.party ::
      Symbol("owner") ->> VA.party ::
      Symbol("currency") ->> VA.text ::
      Symbol("amount") ->> VA.numeric(Scale assertFromInt 10) ::
      Symbol("observers") ->> VA.list(VA.party) ::
      RNil
    VA.record(Ref.Identifier assertFromString "none:Iou:Iou", iouT)
  }

  protected def iouCreateCommand(
      partyName: String,
      amount: String = "999.9900000000",
      currency: String = "USD",
      meta: Option[domain.CommandMeta] = None,
  ): domain.CreateCommand[v.Record, OptionalPkg] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val party = Ref.Party assertFromString partyName
    val arg = argToApi(iouVA)(
      ShRecord(
        issuer = party,
        owner = party,
        currency = currency,
        amount = LfNumeric assertFromString amount,
        observers = Vector.empty,
      )
    )

    domain.CreateCommand(templateId, arg, meta)
  }

  protected def iouExerciseTransferCommand(
      contractId: lar.ContractId
  ): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {
    val templateId = domain.TemplateId(None, "Iou", "Iou")
    val reference = domain.EnrichedContractId(Some(templateId), contractId)
    val arg =
      recordFromFields(ShRecord(newOwner = v.Value.Sum.Party("Bob")))
    val choice = lar.Choice("Iou_Transfer")

    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
  }

  protected def iouCreateAndExerciseTransferCommand(
      partyName: String,
      amount: String = "999.9900000000",
      currency: String = "USD",
  ): domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg] = {
    val templateId: OptionalPkg = domain.TemplateId(None, "Iou", "Iou")
    val party = Ref.Party assertFromString partyName
    val payload = argToApi(iouVA)(
      ShRecord(
        issuer = party,
        owner = party,
        currency = currency,
        amount = LfNumeric assertFromString amount,
        observers = Vector.empty,
      )
    )

    val arg =
      recordFromFields(ShRecord(newOwner = v.Value.Sum.Party("Bob")))
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
    val psv = v.Value.Sum.List(v.List(ps.map(p => v.Value(v.Value.Sum.Party(p)))))
    val payload = recordFromFields(
      ShRecord(
        parties = psv,
        value = v.Value.Sum.Text(value),
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
    val psv = v.Value.Sum.List(v.List(ps.map(p => v.Value(v.Value.Sum.Party(p)))))
    val argument = v.Value(v.Value.Sum.Record(recordFromFields(ShRecord(newParties = psv))))
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
        recordFromFields(
          ShRecord(
            cid = v.Value.Sum.ContractId(fetchedCid.unwrap),
            actors = v.Value.Sum.List(v.List(actors.map(p => v.Value(v.Value.Sum.Party(p))))),
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
      headers: List[HttpHeader] = headersWithAuth,
      readAs: Option[List[domain.Party]] = None,
  ): Future[(StatusCode, JsValue)] =
    for {
      locjson <- toFuture(SprayJson.encode(cmd)): Future[JsValue]
      json <- toFuture(
        readAs.cata(
          ral =>
            SprayJson
              .encode(ral)
              .map(ralj => JsObject(locjson.asJsObject.fields.updated(ReadersKey, ralj))),
          \/-(locjson),
        )
      )
      result <- postJsonRequest(uri.withPath(Uri.Path("/v1/fetch")), json, headers)
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
      decoder: DomainJsonDecoder,
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(jsVal: JsValue): Future[domain.ExerciseCommand[v.Value, domain.EnrichedContractId]] =
    instanceUUIDLogCtx { implicit lc =>
      import scalaz.syntax.bifunctor._
      val cmd =
        decoder.decodeExerciseCommand(jsVal, jwt, ledgerId).getOrElse(fail(s"Cannot decode $jsVal"))
      cmd.map(
        _.bimap(
          lfToApi,
          enrichedContractIdOnly,
        )
      )
    }

  protected def enrichedContractIdOnly(x: domain.ContractLocator[_]): domain.EnrichedContractId =
    x match {
      case a: domain.EnrichedContractId => a
      case _: domain.EnrichedContractKey[_] =>
        fail(s"Expected domain.EnrichedContractId, got: $x")
    }

  protected def lfToApi(lfVal: domain.LfValue): v.Value =
    lfValueToApiValue(verbose = true, lfVal).fold(e => fail(e), identity)

  protected def assertActiveContract(
      decoder: DomainJsonDecoder,
      actual: domain.ActiveContract[JsValue],
      create: domain.CreateCommand[v.Record, OptionalPkg],
      exercise: domain.ExerciseCommand[v.Value, _],
      ledgerId: LedgerId,
  ): Future[Assertion] = {
    import domain.ActiveContractExtras._

    val expectedContractFields: Seq[v.RecordField] = create.payload.fields
    val expectedNewOwner: v.Value = exercise.argument.sum.record
      .flatMap(_.fields.headOption)
      .flatMap(_.value)
      .getOrElse(fail("Cannot extract expected newOwner"))
    instanceUUIDLogCtx(implicit lc =>
      decoder.decodeUnderlyingValues(actual, jwt, ledgerId).valueOr(e => fail(e.shows))
    ).map(active =>
      inside(active.payload.sum.record.map(_.fields)) {
        case Some(
              Seq(
                v.RecordField("iou", Some(contractRecord)),
                v.RecordField("newOwner", Some(newOwner)),
              )
            ) =>
          val contractFields: Seq[v.RecordField] =
            contractRecord.sum.record.map(_.fields).getOrElse(Seq.empty)
          (contractFields: Seq[v.RecordField]) shouldBe (expectedContractFields: Seq[
            v.RecordField
          ])
          (newOwner: v.Value) shouldBe (expectedNewOwner: v.Value)
      }
    )
  }

  protected def assertActiveContract(
      jsVal: JsValue
  )(
      command: domain.CreateCommand[v.Record, OptionalPkg],
      encoder: DomainJsonEncoder,
  ): Future[Assertion] = {

    import encoder.implicits._

    val expected: domain.CreateCommand[JsValue, OptionalPkg] =
      command
        .traversePayload(SprayJson.encode[v.Record](_))
        .getOrElse(fail(s"Failed to encode command: $command"))

    Future {
      inside(SprayJson.decode[domain.ActiveContract[JsValue]](jsVal)) { case \/-(activeContract) =>
        (activeContract.payload: JsValue) shouldBe (expected.payload: JsValue)
      }
    }
  }

  protected def assertTemplateId(
      actual: domain.TemplateId.RequiredPkg,
      expected: OptionalPkg,
  ): Future[Assertion] = Future {
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

  protected def initialIouCreate(
      serviceUri: Uri,
      party: domain.Party,
      headers: List[HttpHeader],
  ): Future[(StatusCode, JsValue)] = {
    val partyJson = party.unwrap
    val payload =
      s"""
        |{
        |  "templateId": "Iou:Iou",
        |  "payload": {
        |    "observers": [],
        |    "issuer": "$partyJson",
        |    "amount": "999.99",
        |    "currency": "USD",
        |    "owner": "$partyJson"
        |  }
        |}
        |""".stripMargin
    HttpServiceTestFixture.postJsonStringRequest(
      serviceUri.withPath(Uri.Path("/v1/create")),
      payload,
      headers,
    )
  }

  protected def initialAccountCreate(
      serviceUri: Uri,
      encoder: DomainJsonEncoder,
      owner: domain.Party,
      headers: List[HttpHeader],
  ): Future[(StatusCode, JsValue)] = {
    val command = accountCreateCommand(owner, "abc123")
    postCreateCommand(command, encoder, serviceUri, headers)
  }

  protected def jsObject(s: String): JsObject = {
    val r: JsonError \/ JsObject = for {
      jsVal <- SprayJson.parse(s).leftMap(e => JsonError(e.shows))
      jsObj <- SprayJson.mustBeJsObject(jsVal)
    } yield jsObj
    r.valueOr(e => fail(e.shows))
  }

  protected def searchExpectOk(
      commands: List[domain.CreateCommand[v.Record, OptionalPkg]],
      query: JsObject,
      uri: Uri,
      encoder: DomainJsonEncoder,
      headers: List[HttpHeader] = headersWithAuth,
  ): Future[List[domain.ActiveContract[JsValue]]] = {
    search(commands, query, uri, encoder, headers).map(expectOk(_))
  }

  protected def search(
      commands: List[domain.CreateCommand[v.Record, OptionalPkg]],
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

  "query GET empty results" in withHttpService { (uri: Uri, _, _, _) =>
    val (_, headers) = getUniquePartyAndAuthHeaders("Alice")
    searchAllExpectOk(uri, headers).flatMap { case vector =>
      vector should have size 0L
    }
  }

  protected def genSearchDataSet(
      party: domain.Party
  ): List[domain.CreateCommand[v.Record, OptionalPkg]] = {
    val partyName = party.unwrap
    List(
      iouCreateCommand(amount = "111.11", currency = "EUR", partyName = partyName),
      iouCreateCommand(amount = "222.22", currency = "EUR", partyName = partyName),
      iouCreateCommand(amount = "333.33", currency = "GBP", partyName = partyName),
      iouCreateCommand(amount = "444.44", currency = "BTC", partyName = partyName),
    )
  }

  "query GET" in withHttpService { (uri: Uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val searchDataSet = genSearchDataSet(alice)
    searchDataSet.traverse(c => postCreateCommand(c, encoder, uri, headers)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

      getRequest(uri = uri.withPath(Uri.Path("/v1/query")), headers)
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

  "multi-party query GET" in withHttpService { (uri, encoder, _, _) =>
    val (alice, aliceHeaders) = getUniquePartyAndAuthHeaders("Alice")
    val (bob, bobHeaders) = getUniquePartyAndAuthHeaders("Alice")
    for {
      _ <- postCreateCommand(
        accountCreateCommand(owner = alice, number = "42"),
        encoder,
        uri,
        headers = aliceHeaders,
      ).map(r => r._1 shouldBe StatusCodes.OK)
      _ <- postCreateCommand(
        accountCreateCommand(owner = bob, number = "23"),
        encoder,
        uri,
        headers = bobHeaders,
      ).map(r => r._1 shouldBe StatusCodes.OK)
      _ <- searchAllExpectOk(uri, aliceHeaders).map(cs => cs should have size 1)
      _ <- searchAllExpectOk(uri, bobHeaders).map(cs => cs should have size 1)
      _ <- searchAllExpectOk(uri, headersWithPartyAuth(List(alice.unwrap, bob.unwrap))).map(cs =>
        cs should have size 2
      )
    } yield succeed
  }

  "query POST with empty query" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val searchDataSet = genSearchDataSet(alice)
    searchExpectOk(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"]}"""),
      uri,
      encoder,
      headers,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe searchDataSet.size
    }
  }

  "multi-party query POST with empty query" in withHttpService { (uri, encoder, _, _) =>
    val (alice, aliceHeaders) = getUniquePartyAndAuthHeaders("Alice")
    val (bob, bobHeaders) = getUniquePartyAndAuthHeaders("Alice")
    for {
      aliceAccountResp <- postCreateCommand(
        accountCreateCommand(owner = alice, number = "42"),
        encoder,
        uri,
        aliceHeaders,
      )
      _ = aliceAccountResp._1 shouldBe StatusCodes.OK
      bobAccountResp <- postCreateCommand(
        accountCreateCommand(owner = bob, number = "23"),
        encoder,
        uri,
        bobHeaders,
      )
      _ = bobAccountResp._1 shouldBe StatusCodes.OK
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        aliceHeaders,
      )
        .map(acl => acl.size shouldBe 1)
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        bobHeaders,
      )
        .map(acl => acl.size shouldBe 1)
      _ <- searchExpectOk(
        List(),
        jsObject("""{"templateIds": ["Account:Account"]}"""),
        uri,
        encoder,
        headers = headersWithPartyAuth(List(alice.unwrap, bob.unwrap)),
      )
        .map(acl => acl.size shouldBe 2)
    } yield {
      assert(true)
    }
  }

  "query with query, one field" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val searchDataSet = genSearchDataSet(alice)
    searchExpectOk(
      searchDataSet,
      jsObject("""{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR"}}"""),
      uri,
      encoder,
      headers,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 2
      acl.map(a => objectField(a.payload, "currency")) shouldBe List.fill(2)(Some(JsString("EUR")))
    }
  }

  "query returns unknown Template IDs as warnings" in withHttpService { (uri, encoder, _, _) =>
    val query =
      jsObject(
        """{"templateIds": ["Iou:Iou", "UnknownModule:UnknownEntity"], "query": {"currency": "EUR"}}"""
      )

    search(List(), query, uri, encoder, headersWithPartyAuth(List("UnknownParty"))).map {
      response =>
        inside(response) { case domain.OkResponse(acl, warnings, StatusCodes.OK) =>
          acl.size shouldBe 0
          warnings shouldBe Some(
            domain.UnknownTemplateIds(
              List(domain.TemplateId(None, "UnknownModule", "UnknownEntity"))
            )
          )
        }
    }
  }

  "query returns unknown Template IDs as warnings and error" in withHttpService {
    (uri, encoder, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      search(
        genSearchDataSet(alice),
        jsObject("""{"templateIds": ["AAA:BBB", "XXX:YYY"]}"""),
        uri,
        encoder,
        headers,
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
    (uri, encoder, _, _) =>
      import scalaz.std.scalaFuture._

      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      val searchDataSet = genSearchDataSet(alice)
      searchDataSet.traverse(c => postCreateCommand(c, encoder, uri, headers)).flatMap {
        rs: List[(StatusCode, JsValue)] =>
          rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)

          def queryAmountAs(s: String) =
            jsObject(s"""{"templateIds": ["Iou:Iou"], "query": {"amount": $s}}""")

          val queryAmountAsString = queryAmountAs("\"111.11\"")
          val queryAmountAsNumber = queryAmountAs("111.11")

          List(
            postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsString, headers),
            postJsonRequest(uri.withPath(Uri.Path("/v1/query")), queryAmountAsNumber, headers),
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

  private[this] def randomTextN(n: Int) = {
    import org.scalacheck.Gen
    Gen
      .buildableOfN[String, Char](n, Gen.alphaNumChar)
      .sample
      .getOrElse(sys.error(s"can't generate ${n}b string"))
  }

  Seq(
    "& " -> "& bar",
    "1kb of data" -> randomTextN(1000),
    "2kb of data" -> randomTextN(2000),
    "3kb of data" -> randomTextN(3000),
    "4kb of data" -> randomTextN(4000),
    "5kb of data" -> randomTextN(5000),
  ).foreach { case (testLbl, testCurrency) =>
    s"query record contains handles '$testLbl' strings properly" in withHttpService {
      (uri, encoder, _, _) =>
        val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
        searchExpectOk(
          genSearchDataSet(alice) :+ iouCreateCommand(
            currency = testCurrency,
            partyName = alice.unwrap,
          ),
          jsObject(
            s"""{"templateIds": ["Iou:Iou"], "query": {"currency": ${testCurrency.toJson}}}"""
          ),
          uri,
          encoder,
          headers,
        ).map(inside(_) { case Seq(domain.ActiveContract(_, _, _, JsObject(fields), _, _, _)) =>
          fields.get("currency") should ===(Some(JsString(testCurrency)))
        })
    }
  }

  "query with query, two fields" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val searchDataSet = genSearchDataSet(alice)
    searchExpectOk(
      searchDataSet,
      jsObject(
        """{"templateIds": ["Iou:Iou"], "query": {"currency": "EUR", "amount": "111.11"}}"""
      ),
      uri,
      encoder,
      headers,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 1
      acl.map(a => objectField(a.payload, "currency")) shouldBe List(Some(JsString("EUR")))
      acl.map(a => objectField(a.payload, "amount")) shouldBe List(Some(JsString("111.11")))
    }
  }

  "query with query, no results" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val searchDataSet = genSearchDataSet(alice)
    searchExpectOk(
      searchDataSet,
      jsObject(
        """{"templateIds": ["Iou:Iou"], "query": {"currency": "RUB", "amount": "666.66"}}"""
      ),
      uri,
      encoder,
      headers,
    ).map { acl: List[domain.ActiveContract[JsValue]] =>
      acl.size shouldBe 0
    }
  }

  "query with invalid JSON query should return error" in withHttpService { (uri, _, _, _) =>
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

  "create IOU" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)

    postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val activeContract = getResult(output)
      assertActiveContract(activeContract)(command, encoder)
    }: Future[Assertion]
  }

  "create IOU should fail if authorization header is missing" in withHttpService {
    (uri, encoder, _, _) =>
      val alice = getUniqueParty("Alice")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input, List()).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          expectedOneErrorMessage(output) should include(
            "missing Authorization header with OAuth 2.0 Bearer Token"
          )
      }: Future[Assertion]
  }

  "create IOU should support extra readAs parties" in withHttpService { (uri, encoder, _, _) =>
    val alice = getUniqueParty("Alice")
    val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
    val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

    postJsonRequest(
      uri.withPath(Uri.Path("/v1/create")),
      input,
      headers = headersWithPartyAuth(actAs = List(alice.unwrap), readAs = List("Bob")),
    ).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val activeContract = getResult(output)
      assertActiveContract(activeContract)(command, encoder)
    }: Future[Assertion]
  }

  "create IOU with unsupported templateId should return proper error" in withHttpService {
    (uri, encoder, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      val command: domain.CreateCommand[v.Record, OptionalPkg] =
        iouCreateCommand(alice.unwrap).copy(templateId = domain.TemplateId(None, "Iou", "Dummy"))
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))

      postJsonRequest(uri.withPath(Uri.Path("/v1/create")), input, headers).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
          val unknownTemplateId: OptionalPkg =
            domain.TemplateId(None, command.templateId.moduleName, command.templateId.entityName)
          expectedOneErrorMessage(output) should include(
            s"Cannot resolve template ID, given: ${unknownTemplateId: OptionalPkg}"
          )
      }: Future[Assertion]
  }

  "exercise IOU_Transfer" in withHttpService { (uri, encoder, decoder, ledgerId) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val create: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
    postCreateCommand(create, encoder, uri, headers)
      .flatMap { case (createStatus, createOutput) =>
        createStatus shouldBe StatusCodes.OK
        assertStatus(createOutput, StatusCodes.OK)

        val contractId = getContractId(getResult(createOutput))
        val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
          iouExerciseTransferCommand(contractId)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson, headers)
          .flatMap { case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
            assertExerciseResponseNewActiveContract(
              getResult(exerciseOutput),
              create,
              exercise,
              decoder,
              uri,
              ledgerId,
              headers,
            )
          }
      }: Future[Assertion]
  }

  "create-and-exercise IOU_Transfer" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val cmd: domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg] =
      iouCreateAndExerciseTransferCommand(alice.unwrap)

    val json: JsValue = encoder.encodeCreateAndExerciseCommand(cmd).valueOr(e => fail(e.shows))

    postJsonRequest(uri.withPath(Uri.Path("/v1/create-and-exercise")), json, headers)
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
      createCmd: domain.CreateCommand[v.Record, OptionalPkg],
      exerciseCmd: domain.ExerciseCommand[v.Value, domain.EnrichedContractId],
      decoder: DomainJsonDecoder,
      uri: Uri,
      ledgerId: LedgerId,
      headers: List[HttpHeader],
  ): Future[Assertion] = {
    inside(SprayJson.decode[domain.ExerciseResponse[JsValue]](exerciseResponse)) {
      case \/-(domain.ExerciseResponse(JsString(exerciseResult), List(contract1, contract2))) =>
        // checking contracts
        inside(contract1) { case domain.Contract(-\/(archivedContract)) =>
          Future {
            (archivedContract.contractId.unwrap: String) shouldBe (exerciseCmd.reference.contractId.unwrap: String)
          }
        } *>
          inside(contract2) { case domain.Contract(\/-(activeContract)) =>
            assertActiveContract(decoder, activeContract, createCmd, exerciseCmd, ledgerId)
          } *>
          // checking exerciseResult
          {
            exerciseResult.length should be > (0)
            val newContractLocator = domain.EnrichedContractId(
              Some(domain.TemplateId(None, "Iou", "IouTransfer")),
              domain.ContractId(exerciseResult),
            )
            postContractsLookup(newContractLocator, uri, headers).flatMap { case (status, output) =>
              status shouldBe StatusCodes.OK
              assertStatus(output, StatusCodes.OK)
              getContractId(getResult(output)) shouldBe newContractLocator.contractId
            }: Future[Assertion]
          }
    }
  }

  "exercise IOU_Transfer with unknown contractId should return proper error" in withHttpService {
    (uri, encoder, _, _) =>
      val contractIdString = "0" * 66
      val contractId = lar.ContractId(contractIdString)
      val exerciseJson: JsValue = encodeExercise(encoder)(iouExerciseTransferCommand(contractId))
      postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson)
        .flatMap { case (status, output) =>
          status shouldBe StatusCodes.NotFound
          assertStatus(output, StatusCodes.NotFound)
          expectedOneErrorMessage(output) should include(
            s"Contract could not be found with id $contractIdString"
          )
        }: Future[Assertion]
  }

  "exercise Archive" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val create: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
    postCreateCommand(create, encoder, uri, headers)
      .flatMap { case (createStatus, createOutput) =>
        createStatus shouldBe StatusCodes.OK
        assertStatus(createOutput, StatusCodes.OK)

        val contractId = getContractId(getResult(createOutput))
        val templateId = domain.TemplateId(None, "Iou", "Iou")
        val reference = domain.EnrichedContractId(Some(templateId), contractId)
        val exercise = archiveCommand(reference)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), exerciseJson, headers)
          .flatMap { case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
            val exercisedResponse: JsObject = getResult(exerciseOutput).asJsObject
            assertExerciseResponseArchivedContract(exercisedResponse, exercise)
          }
      }: Future[Assertion]
  }

  "should support multi-party command submissions" in withHttpService { (uri, encoder, _, _) =>
    for {
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

  "should be able to serialize and deserialize domain commands" in withLedger {
    (client, ledgerId) =>
      instanceUUIDLogCtx(implicit lc =>
        jsonCodecs(client, ledgerId, None).flatMap { case (encoder, decoder) =>
          testCreateCommandEncodingDecoding(encoder, decoder, ledgerId) *>
            testExerciseCommandEncodingDecoding(
              encoder,
              decoder,
              ledgerId,
            )
        }: Future[Assertion]
      )
  }

  private def testCreateCommandEncodingDecoding(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      ledgerId: LedgerId,
  ): Future[Assertion] = instanceUUIDLogCtx { implicit lc =>
    import json.JsonProtocol._
    import util.ErrorOps._

    val command0: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand("Alice")

    type F[A] = EitherT[Future, JsonError, A]
    val x: F[Assertion] = for {
      jsVal <- EitherT.either(
        encoder.encodeCreateCommand(command0).liftErr(JsonError)
      ): F[JsValue]
      command1 <- decoder.decodeCreateCommand(jsVal, jwt, ledgerId)
    } yield command1.bimap(removeRecordId, removePackageId) should ===(command0)

    (x.run: Future[JsonError \/ Assertion]).map(_.fold(e => fail(e.shows), identity))
  }

  private def testExerciseCommandEncodingDecoding(
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      ledgerId: LedgerId,
  ): Future[Assertion] = {
    val command0 = iouExerciseTransferCommand(lar.ContractId("#a-contract-ID"))
    val jsVal: JsValue = encodeExercise(encoder)(command0)
    val command1 = decodeExercise(decoder, jwt, ledgerId)(jsVal)
    command1.map(_.bimap(removeRecordId, identity) should ===(command0))
  }

  "request non-existent endpoint should return 404 with errors" in withHttpService {
    (uri: Uri, _, _, _) =>
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
    (uri, _, _, client, _) =>
      import scalaz.std.vector._
      val partyIds = Vector("P1", "P2", "P3", "P4")
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
              actualIds should contain allElementsOf domain.Party.subst(partyIds.toSet)
              response.result.toSet should contain allElementsOf
                allocatedParties.toSet.map(domain.PartyDetails.fromLedgerApi)
            }
          }
        }: Future[Assertion]
  }

  "parties endpoint should return only requested parties, unknown parties returned as warnings" in withHttpServiceAndClient {
    (uri, _, _, client, _) =>
      import scalaz.std.vector._

      val charlie = getUniqueParty("Charlie")
      val knownParties = Vector(getUniqueParty("Alice"), getUniqueParty("Bob")) :+ charlie
      val erin = getUniqueParty("Erin")
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
    (uri, _, _, _, _) =>
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
    (uri, _, _, _, _) =>
      val requestedPartyIds: Vector[domain.Party] = domain.Party.subst(Vector(""))

      postJsonRequest(
        uri = uri.withPath(Uri.Path("/v1/parties")),
        JsArray(requestedPartyIds.map(x => JsString(x.unwrap))),
      ).flatMap { case (status, output) =>
        status shouldBe StatusCodes.BadRequest
        inside(decode1[domain.SyncResponse, List[domain.PartyDetails]](output)) {
          case \/-(domain.ErrorResponse(List(error), None, StatusCodes.BadRequest)) =>
            error should include("Daml-LF Party is empty")
        }
      }: Future[Assertion]
  }

  "parties endpoint returns empty result with warnings and OK status if nothing found" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      val requestedPartyIds: Vector[domain.Party] =
        Vector(getUniqueParty("Alice"), getUniqueParty("Bob"))
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

  "parties/allocate should allocate a new party" in withHttpServiceAndClient { (uri, _, _, _, _) =>
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
    (uri, _, _, _, _) =>
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
    (uri, _, _, _, _) =>
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

  "fetch by contractId" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)

    postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val contractId: ContractId = getContractId(getResult(output))
      val locator = domain.EnrichedContractId(None, contractId)
      lookupContractAndAssert(locator)(contractId, command, encoder, uri, headers)
    }: Future[Assertion]
  }

  "fetch returns {status:200, result:null} when contract is not found" in withHttpService {
    (uri, _, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      val accountNumber = "abc123"
      val locator = domain.EnrichedContractKey(
        domain.TemplateId(None, "Account", "Account"),
        JsArray(JsString(alice.unwrap), JsString(accountNumber)),
      )
      postContractsLookup(locator, uri.withPath(Uri.Path("/v1/fetch")), headers).flatMap {
        case (status, output) =>
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          output
            .asJsObject(s"expected JsObject, got: $output")
            .fields
            .get("result") shouldBe Some(JsNull)
      }: Future[Assertion]
  }

  "fetch fails when readAs not authed, even if prior fetch succeeded" in withHttpService {
    (uri, encoder, _, _) =>
      val (alice, aliceHeaders) = getUniquePartyAndAuthHeaders("Alice")
      val command = iouCreateCommand(alice.unwrap)
      for {
        createStatusOutput <- postCreateCommand(command, encoder, uri, aliceHeaders)
        contractId = {
          val (status, output) = createStatusOutput
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getContractId(getResult(output))
        }
        locator = domain.EnrichedContractId(None, contractId)
        // will cache if DB configured
        _ <- lookupContractAndAssert(locator)(contractId, command, encoder, uri, aliceHeaders)
        charlie = getUniqueParty("Charlie")
        badLookup <- postContractsLookup(
          locator,
          uri.withPath(Uri.Path("/v1/fetch")),
          aliceHeaders,
          readAs = Some(List(charlie)),
        )
        _ = {
          val (status, output) = badLookup
          status shouldBe StatusCodes.Unauthorized
          assertStatus(output, StatusCodes.Unauthorized)
          output
            .asJsObject(s"expected JsObject, got: $output")
            .fields
            .keySet should ===(Set("errors", "status"))
        }
      } yield succeed
  }

  "fetch by key" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val accountNumber = "abc123"
    val command: domain.CreateCommand[v.Record, OptionalPkg] =
      accountCreateCommand(alice, accountNumber)

    postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      val contractId: ContractId = getContractId(getResult(output))
      val locator = domain.EnrichedContractKey(
        domain.TemplateId(None, "Account", "Account"),
        JsArray(JsString(alice.unwrap), JsString(accountNumber)),
      )
      lookupContractAndAssert(locator)(contractId, command, encoder, uri, headers)
    }: Future[Assertion]
  }

  "commands/exercise Archive by key" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val accountNumber = "abc123"
    val create: domain.CreateCommand[v.Record, OptionalPkg] =
      accountCreateCommand(alice, accountNumber)

    val keyRecord = v.Record(
      fields = Seq(
        v.RecordField(value = Some(v.Value(v.Value.Sum.Party(alice.unwrap)))),
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

    postCreateCommand(create, encoder, uri, headers).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)

      postJsonRequest(uri.withPath(Uri.Path("/v1/exercise")), archiveJson, headers).flatMap {
        case (exerciseStatus, exerciseOutput) =>
          exerciseStatus shouldBe StatusCodes.OK
          assertStatus(exerciseOutput, StatusCodes.OK)
      }
    }: Future[Assertion]
  }

  "fetch by key containing variant and record, encoded as array with number num" in withHttpService {
    (uri, _, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      testFetchByCompositeKey(
        uri,
        jsObject(s"""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": [
              "$alice",
              {"tag": "Bar", "value": 42},
              {"baz": "another baz value"}
            ]
          }"""),
        alice,
        headers,
      )
  }

  "fetch by key containing variant and record, encoded as record with string num" in withHttpService {
    (uri, _, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      testFetchByCompositeKey(
        uri,
        jsObject(s"""{
            "templateId": "Account:KeyedByVariantAndRecord",
            "key": {
              "_1": "$alice",
              "_2": {"tag": "Bar", "value": "42"},
              "_3": {"baz": "another baz value"}
            }
          }"""),
        alice,
        headers,
      )
  }

  private def testFetchByCompositeKey(
      uri: Uri,
      request: JsObject,
      party: domain.Party,
      headers: List[HttpHeader],
  ) = {
    val createCommand = jsObject(s"""{
        "templateId": "Account:KeyedByVariantAndRecord",
        "payload": {
          "name": "ABC DEF",
          "party": "${party.unwrap}",
          "age": 123,
          "fooVariant": {"tag": "Bar", "value": 42},
          "bazRecord": {"baz": "another baz value"}
        }
      }""")
    postJsonRequest(uri.withPath(Uri.Path("/v1/create")), createCommand, headers).flatMap {
      case (status, output) =>
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        val contractId: ContractId = getContractId(getResult(output))

        postJsonRequest(uri.withPath(Uri.Path("/v1/fetch")), request, headers).flatMap {
          case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            activeContract(output).contractId shouldBe contractId
        }
    }: Future[Assertion]
  }

  "query by a variant field" in withHttpService { (uri, encoder, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    val accountNumber = "abc123"
    val now = TimestampConversion.roundInstantToMicros(Instant.now)
    val nowStr = TimestampConversion.microsToInstant(now).toString
    val command: domain.CreateCommand[v.Record, OptionalPkg] =
      accountCreateCommand(alice, accountNumber, now)

    val packageId: Ref.PackageId = MetadataReader
      .templateByName(metadata2)(Ref.QualifiedName.assertFromString("Account:Account"))
      .headOption
      .map(_._1)
      .getOrElse(fail(s"Cannot retrieve packageId"))

    postCreateCommand(command, encoder, uri, headers).flatMap { case (status, output) =>
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

      postJsonRequest(uri.withPath(Uri.Path("/v1/query")), query, headers).map {
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
    (uri, _, _, _, _) =>
      getAllPackageIds(uri).map { x =>
        inside(x) {
          case domain.OkResponse(ps, None, StatusCodes.OK) if ps.nonEmpty =>
            Inspectors.forAll(ps)(_.length should be > 0)
        }
      }: Future[Assertion]
  }

  "packages/packageId should return a requested package" in withHttpServiceAndClient {
    import AbstractHttpServiceIntegrationTestFuns.sha256
    (uri, _, _, _, _) =>
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

  "packages upload endpoint" in withHttpServiceAndClient { (uri, _, _, _, _) =>
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

  "package list is updated when a query request is made" in withLedger2[Assertion] {
    (ledgerPort: Port, _, _) =>
      val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
      for {
        _ <- withHttpServiceOnly(ledgerPort) { (uri, encoder, _) =>
          val searchDataSet = genSearchDataSet(alice)
          searchDataSet.traverse(c => postCreateCommand(c, encoder, uri, headers)).flatMap { rs =>
            rs.map(_._1) shouldBe List.fill(searchDataSet.size)(StatusCodes.OK)
          }
        }
        _ <- withHttpServiceOnly(ledgerPort) { (uri, _, _) =>
          getRequest(uri = uri.withPath(Uri.Path("/v1/query")), headers)
            .flatMap { case (status, output) =>
              status shouldBe StatusCodes.OK
              assertStatus(output, StatusCodes.OK)
              inside(getResult(output)) { case JsArray(result) =>
                result should have length 4
              }
            }: Future[Assertion]
        }
      } yield succeed
  }

  "archiving a large number of contracts should succeed" in withHttpServiceAndClient(
    StartSettings.DefaultMaxInboundMessageSize * 10
  ) { (uri, encoder, _, _, _) =>
    val (alice, headers) = getUniquePartyAndAuthHeaders("Alice")
    //The numContracts size should test for https://github.com/digital-asset/daml/issues/10339
    val numContracts: Long = 2000
    val helperId = domain.TemplateId(None, "Account", "Helper")
    val payload = v.Record(
      fields = List(v.RecordField("owner", Some(v.Value(v.Value.Sum.Party(alice.unwrap)))))
    )
    val createCmd: domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg] =
      domain.CreateAndExerciseCommand(
        templateId = helperId,
        payload = payload,
        choice = lar.Choice("CreateN"),
        argument = boxedRecord(
          v.Record(fields =
            List(v.RecordField("n", Some(v.Value(v.Value.Sum.Int64(numContracts)))))
          )
        ),
        meta = None,
      )
    def encode(cmd: domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg]): JsValue =
      encoder.encodeCreateAndExerciseCommand(cmd).valueOr(e => fail(e.shows))
    def archiveCmd(cids: List[String]) =
      domain.CreateAndExerciseCommand(
        templateId = helperId,
        payload = payload,
        choice = lar.Choice("ArchiveAll"),
        argument = boxedRecord(
          v.Record(fields =
            List(
              v.RecordField(
                "cids",
                Some(
                  v.Value(
                    v.Value.Sum.List(v.List(cids.map(cid => v.Value(v.Value.Sum.ContractId(cid)))))
                  )
                ),
              )
            )
          )
        ),
        meta = None,
      )
    def queryN(n: Long): Future[Assertion] = postJsonRequest(
      uri.withPath(Uri.Path("/v1/query")),
      jsObject("""{"templateIds": ["Account:Account"]}"""),
      headers,
    ).flatMap { case (status, output) =>
      status shouldBe StatusCodes.OK
      assertStatus(output, StatusCodes.OK)
      inside(getResult(output)) { case JsArray(result) =>
        result should have length n
      }
    }

    for {
      resp <- postJsonRequest(
        uri.withPath(Uri.Path("/v1/create-and-exercise")),
        encode(createCmd),
        headers,
      )
      (status, output) = resp
      _ = {
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
      }
      created = getChild(getResult(output), "exerciseResult").convertTo[List[String]]
      _ = created should have length numContracts

      _ <- queryN(numContracts)

      status <- postJsonRequest(
        uri.withPath(Uri.Path("/v1/create-and-exercise")),
        encode(archiveCmd(created)),
        headers,
      ).map(_._1)
      _ = {
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
      }

      _ <- queryN(0)
    } yield succeed
  }

  "Should ignore conflicts on contract key hash constraint violation" in withHttpServiceAndClient {
    (uri, encoder, _, _, _) =>
      import scalaz.std.vector._
      import scalaz.syntax.tag._
      import scalaz.syntax.traverse._
      import scalaz.std.scalaFuture._
      import shapeless.record.{Record => ShRecord}
      import com.daml.ledger.api.refinements.{ApiTypes => lar}

      val partyIds = Vector("Alice", "Bob").map(getUniqueParty)
      val packageId: Ref.PackageId = MetadataReader
        .templateByName(metadataUser)(Ref.QualifiedName.assertFromString("User:User"))
        .collectFirst { case (pkgid, _) => pkgid }
        .getOrElse(fail(s"Cannot retrieve packageId"))

      def userCreateCommand(
          username: domain.Party,
          following: Seq[domain.Party] = Seq.empty,
      ): domain.CreateCommand[v.Record, domain.TemplateId.OptionalPkg] = {
        val templateId = domain.TemplateId(None, "User", "User")
        val followingList = following.map(party => v.Value(v.Value.Sum.Party(party.unwrap)))
        val arg = recordFromFields(
          ShRecord(
            username = v.Value.Sum.Party(username.unwrap),
            following = v.Value.Sum.List(v.List.of(followingList)),
          )
        )

        domain.CreateCommand(templateId, arg, None)
      }
      def userExerciseFollowCommand(
          contractId: lar.ContractId,
          toFollow: domain.Party,
      ): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {
        val templateId = domain.TemplateId(None, "User", "User")
        val reference = domain.EnrichedContractId(Some(templateId), contractId)
        val arg = recordFromFields(ShRecord(userToFollow = v.Value.Sum.Party(toFollow.unwrap)))
        val choice = lar.Choice("Follow")

        domain.ExerciseCommand(reference, choice, boxedRecord(arg), None)
      }

      def followUser(contractId: lar.ContractId, actAs: domain.Party, toFollow: domain.Party) = {
        val exercise: domain.ExerciseCommand[v.Value, domain.EnrichedContractId] =
          userExerciseFollowCommand(contractId, toFollow)
        val exerciseJson: JsValue = encodeExercise(encoder)(exercise)

        postJsonRequest(
          uri.withPath(Uri.Path("/v1/exercise")),
          exerciseJson,
          headers = headersWithPartyAuth(actAs = List(actAs.unwrap)),
        )
          .map { case (exerciseStatus, exerciseOutput) =>
            exerciseStatus shouldBe StatusCodes.OK
            assertStatus(exerciseOutput, StatusCodes.OK)
            ()
          }

      }

      def queryUsers(fromPerspectiveOfParty: domain.Party) = {
        val query = jsObject(s"""{
             "templateIds": ["$packageId:User:User"],
             "query": {}
          }""")

        postJsonRequest(
          uri.withPath(Uri.Path("/v1/query")),
          query,
          headers = headersWithPartyAuth(actAs = List(fromPerspectiveOfParty.unwrap)),
        ).map { case (searchStatus, searchOutput) =>
          searchStatus shouldBe StatusCodes.OK
          assertStatus(searchOutput, StatusCodes.OK)
        }
      }
      val commands = partyIds.map { p =>
        (p, userCreateCommand(p))
      }

      for {
        users <- commands.traverse { case (party, command) =>
          val fut = postCreateCommand(
            command,
            encoder,
            uri,
            headers = headersWithPartyAuth(actAs = List(party.unwrap)),
          ).map { case (status, output) =>
            status shouldBe StatusCodes.OK
            assertStatus(output, StatusCodes.OK)
            getContractId(getResult(output))
          }: Future[ContractId]
          fut.map(cid => (party, cid))
        }
        (alice, aliceUserId) = users(0)
        (bob, bobUserId) = users(1)
        _ <- followUser(aliceUserId, alice, bob)
        _ <- queryUsers(bob)
        _ <- followUser(bobUserId, bob, alice)
        _ <- queryUsers(alice)
      } yield succeed
  }
}
