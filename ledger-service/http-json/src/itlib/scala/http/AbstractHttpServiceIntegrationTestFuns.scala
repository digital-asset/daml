// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.security.DigestInputStream
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Authorization
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.crypto.MessageDigestPrototype
import com.daml.lf.data.Ref
import com.daml.http.dbbackend.JdbcConfig
import com.daml.http.domain.ContractId
import com.daml.http.domain.TemplateId.OptionalPkg
import com.daml.http.json.SprayJson.decode1
import com.daml.http.json._
import com.daml.http.util.ClientUtil.boxedRecord
import com.daml.http.util.FutureUtil.toFuture
import com.daml.http.util.{FutureUtil, SandboxTestLedger}
import com.daml.jwt.domain.Jwt
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
import org.scalatest.matchers.should.Matchers
import scalaz.std.list._
import scalaz.std.scalaFuture._
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.{\/, \/-}
import shapeless.record.{Record => ShRecord}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import com.daml.ledger.api.{domain => LedgerApiDomain}
import com.daml.lf.{value => lfv}
import lfv.test.TypedValueGenerators.{ValueAddend => VA}
import com.daml.ports.Port

object AbstractHttpServiceIntegrationTestFuns {
  private[http] val dar1 = requiredResource(ModelTestDar.path)

  private[http] val dar2 = requiredResource("ledger-service/http-json/Account.dar")

  private[http] val dar3 = requiredResource(SemanticTestDar.path)

  private[http] val userDar = requiredResource("ledger-service/http-json/User.dar")

  private[http] val ciouDar = requiredResource("ledger-service/http-json/CIou.dar")

  def sha256(source: Source[ByteString, Any])(implicit mat: Materializer): Try[String] = Try {
    import com.google.common.io.BaseEncoding

    val md = MessageDigestPrototype.Sha256.newDigest
    val is = source.runWith(StreamConverters.asInputStream())
    val dis = new DigestInputStream(is, md)

    // drain the input stream and calculate the hash
    while (-1 != dis.read()) ()

    dis.on(false)

    BaseEncoding.base16().lowerCase().encode(md.digest())
  }

  // ValueAddend eXtensions
  private[http] object VAx {
    def seq(elem: VA): VA.Aux[Seq[elem.Inj]] =
      VA.list(elem).xmap { xs: Seq[elem.Inj] => xs }(_.toVector)

    // nest assertFromString into arbitrary VA structures
    val partyStr: VA.Aux[String] = VA.party.xmap(identity[String])(Ref.Party.assertFromString)

    val partyDomain: VA.Aux[domain.Party] = domain.Party.subst[VA.Aux, String](partyStr)
  }

  private[http] trait UriFixture {
    def uri: Uri
  }
  private[http] trait EncoderFixture {
    def encoder: DomainJsonEncoder
  }
  private[http] sealed trait DecoderFixture {
    def decoder: DomainJsonDecoder
  }

  private[http] final case class HttpServiceOnlyTestFixtureData(
      uri: Uri,
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
  ) extends UriFixture
      with EncoderFixture
      with DecoderFixture

  private[http] final case class HttpServiceTestFixtureData(
      uri: Uri,
      encoder: DomainJsonEncoder,
      decoder: DomainJsonDecoder,
      client: DamlLedgerClient,
      ledgerId: LedgerId,
  ) extends UriFixture
      with EncoderFixture
      with DecoderFixture
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait AbstractHttpServiceIntegrationTestFuns
    extends StrictLogging
    with HttpServiceUserFixture
    with SandboxTestLedger
    with SuiteResourceManagementAroundAll {
  this: AsyncTestSuite with Matchers with Inside =>

  import AbstractHttpServiceIntegrationTestFuns._
  import HttpServiceTestFixture._
  import json.JsonProtocol._

  def jdbcConfig: Option[JdbcConfig]

  def staticContentConfig: Option[StaticContentConfig]

  def useTls: UseTls

  def wsConfig: Option[WebsocketConfig]

  protected def testId: String = this.getClass.getSimpleName

  protected val metadata2: MetadataReader.LfMetadata =
    MetadataReader.readFromDar(dar2).valueOr(e => fail(s"Cannot read dar2 metadata: $e"))

  protected val metadataUser: MetadataReader.LfMetadata =
    MetadataReader.readFromDar(userDar).valueOr(e => fail(s"Cannot read userDar metadata: $e"))

  protected def jwt(uri: Uri)(implicit ec: ExecutionContext): Future[Jwt]

  override def packageFiles = List(dar1, dar2, userDar)

  protected def withHttpServiceAndClient[A](
      testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, DamlLedgerClient, LedgerId) => Future[A]
  ): Future[A] =
    withHttpService() { case HttpServiceTestFixtureData(a, b, c, d, e) => testFn(a, b, c, d, e) }

  protected def withHttpService[A](
      token: Option[Jwt] = None,
      maxInboundMessageSize: Int = StartSettings.DefaultMaxInboundMessageSize,
  )(
      testFn: HttpServiceTestFixtureData => Future[A]
  ): Future[A] = usingLedger[A](testId, token map (_.value)) { case (ledgerPort, _, ledgerId) =>
    HttpServiceTestFixture.withHttpService[A](
      testId,
      ledgerPort,
      jdbcConfig,
      staticContentConfig,
      useTls = useTls,
      wsConfig = wsConfig,
      maxInboundMessageSize = maxInboundMessageSize,
      token = token orElse Some(jwtAdminNoParty),
    )((u, e, d, c) => testFn(HttpServiceTestFixtureData(u, e, d, c, ledgerId)))
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

  protected def withHttpService[A](f: HttpServiceTestFixtureData => Future[A]): Future[A] =
    withHttpService()(f)

  protected def withHttpServiceOnly[A](ledgerPort: Port)(
      f: HttpServiceOnlyTestFixtureData => Future[A]
  ): Future[A] =
    HttpServiceTestFixture.withHttpService[A](
      testId,
      ledgerPort,
      jdbcConfig,
      staticContentConfig,
      useTls = useTls,
      wsConfig = wsConfig,
      token = Some(jwtAdminNoParty),
    )((uri, encoder, decoder, _) => f(HttpServiceOnlyTestFixtureData(uri, encoder, decoder)))

  protected def withLedger[A](testFn: (DamlLedgerClient, LedgerId) => Future[A]): Future[A] =
    usingLedger[A](testId, token = Some(jwtAdminNoParty.value)) { case (_, client, ledgerId) =>
      testFn(client, ledgerId)
    }

  implicit protected final class `AHS Funs Uri functions`(private val self: UriFixture) {

    import self.uri

    def getUniquePartyAndAuthHeaders(
        name: String
    ): Future[(domain.Party, List[HttpHeader])] = {
      val domain.Party(partyName) = getUniqueParty(name)
      headersWithPartyAuth(List(partyName), List.empty, "").map(token =>
        (domain.Party(partyName), token)
      )
    }

    def headersWithAuth(implicit ec: ExecutionContext): Future[List[Authorization]] =
      jwt(uri)(ec).map(authorizationHeader)

    def headersWithPartyAuth(
        actAs: List[String],
        readAs: List[String] = List.empty,
        ledgerId: String = "",
        withoutNamespace: Boolean = false,
        admin: Boolean = false,
    )(implicit ec: ExecutionContext): Future[List[Authorization]] =
      jwtForParties(uri)(actAs, readAs, ledgerId, withoutNamespace, admin)(ec)
        .map(authorizationHeader)

    def postJsonStringRequest(
        path: Uri.Path,
        jsonString: String,
    ): Future[(StatusCode, JsValue)] =
      headersWithAuth.flatMap(
        HttpServiceTestFixture.postJsonStringRequest(uri withPath path, jsonString, _)
      )

    def postJsonRequest(
        path: Uri.Path,
        json: JsValue,
        headers: List[HttpHeader],
    ): Future[(StatusCode, JsValue)] =
      HttpServiceTestFixture.postJsonRequest(uri withPath path, json, headers)

    // XXX SC check that the status matches the one in the SyncResponse, and
    // remove StatusCode from these responses everywhere, if all tests are
    // similarly duplicative

    def postJsonRequestWithMinimumAuth[Result: JsonReader](
        path: Uri.Path,
        json: JsValue,
    ): Future[(StatusCode, domain.SyncResponse[Result])] =
      headersWithAuth
        .flatMap(postJsonRequest(path, json, _))
        .parseResponse[Result]

    def getRequest(
        path: Uri.Path,
        headers: List[HttpHeader],
    ): Future[(StatusCode, JsValue)] =
      HttpServiceTestFixture
        .getRequest(uri withPath path, headers)

    def getRequestWithMinimumAuth[Result: JsonReader](
        path: Uri.Path
    ): Future[(StatusCode, domain.SyncResponse[Result])] =
      headersWithAuth
        .flatMap(getRequest(path, _))
        .parseResponse[Result]
  }

  implicit protected final class `Future JsValue functions`[A](
      private val self: Future[(A, JsValue)]
  ) {
    def parseResponse[Result: JsonReader]: Future[(A, domain.SyncResponse[Result])] =
      self.map(_ map (decode1[domain.SyncResponse, Result](_).fold(e => fail(e.shows), identity)))
  }

  protected def postCreateCommand(
      cmd: domain.CreateCommand[v.Record, OptionalPkg],
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[(StatusCode, domain.SyncResponse[domain.ActiveContract[JsValue]])] =
    HttpServiceTestFixture
      .postCreateCommand(cmd, fixture.encoder, fixture.uri, headers)
      .parseResponse[domain.ActiveContract[JsValue]]

  protected def postCreateCommand(
      cmd: domain.CreateCommand[v.Record, OptionalPkg],
      fixture: UriFixture with EncoderFixture,
  ): Future[(StatusCode, domain.SyncResponse[domain.ActiveContract[JsValue]])] =
    fixture.headersWithAuth.flatMap(postCreateCommand(cmd, fixture, _))

  protected def postArchiveCommand(
      templateId: OptionalPkg,
      contractId: domain.ContractId,
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[(StatusCode, JsValue)] =
    HttpServiceTestFixture.postArchiveCommand(
      templateId,
      contractId,
      fixture.encoder,
      fixture.uri,
      headers,
    )

  protected def postArchiveCommand(
      templateId: OptionalPkg,
      contractId: domain.ContractId,
      fixture: UriFixture with EncoderFixture,
  ): Future[(StatusCode, JsValue)] =
    fixture.headersWithAuth.flatMap(
      postArchiveCommand(templateId, contractId, fixture, _)
    )

  protected def lookupContractAndAssert(
      contractLocator: domain.ContractLocator[JsValue],
      contractId: ContractId,
      create: domain.CreateCommand[v.Record, OptionalPkg],
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[Assertion] =
    postContractsLookup(contractLocator, fixture.uri, headers).map(inside(_) {
      case (StatusCodes.OK, domain.OkResponse(Some(resultContract), _, StatusCodes.OK)) =>
        contractId shouldBe resultContract.contractId
        assertActiveContract(resultContract)(create, fixture.encoder)
    })

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
  import shapeless.HList

  private[this] object RecordFromFields extends shapeless.Poly1 {
    import shapeless.Witness
    import shapeless.labelled.{FieldType => :->>:}

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

  protected[this] def argToApi(va: VA)(arg: va.Inj): v.Record =
    lfToApi(va.inj(arg)) match {
      case v.Value(v.Value.Sum.Record(r)) => removeRecordId(r)
      case _ => fail(s"${va.t} isn't a record type")
    }

  private[this] val (_, iouVA) = {
    import com.daml.lf.data.Numeric.Scale
    val iouT = ShRecord(
      issuer = VA.party,
      owner = VA.party,
      currency = VA.text,
      amount = VA.numeric(Scale assertFromInt 10),
      observers = VA.list(VA.party),
    )
    VA.record(Ref.Identifier assertFromString "none:Iou:Iou", iouT)
  }

  protected[this] object TpId {
    import domain.TemplateId.{OptionalPkg => Id}
    import domain.{ContractTypeId => CtId}
    import CtId.Template.{OptionalPkg => TId}
    import CtId.Interface.{OptionalPkg => IId}

    object Iou {
      val Iou: Id = domain.TemplateId(None, "Iou", "Iou")
      val IouTransfer: Id = domain.TemplateId(None, "Iou", "IouTransfer")
    }
    object Test {
      val MultiPartyContract: Id = domain.TemplateId(None, "Test", "MultiPartyContract")
    }
    object Account {
      val Account: TId = CtId.Template(None, "Account", "Account")
    }
    object User {
      val User: Id = domain.TemplateId(None, "User", "User")
    }
    object IIou {
      val IIou: IId = CtId.Interface(None, "IIou", "IIou")
    }
    object RIIou {
      val RIIou: IId = CtId.Interface(None, "RIIou", "RIIou")
    }

    def unsafeCoerce[Like[T] <: CtId[T], T](ctId: CtId[T])(implicit
        Like: CtId.Like[Like]
    ): Like[T] =
      Like(ctId.packageId, ctId.moduleName, ctId.entityName)
  }

  protected def iouCreateCommand(
      partyName: domain.Party,
      amount: String = "999.9900000000",
      currency: String = "USD",
      meta: Option[domain.CommandMeta] = None,
  ): domain.CreateCommand[v.Record, OptionalPkg] = {
    val party = Ref.Party assertFromString partyName.unwrap
    val arg = argToApi(iouVA)(
      ShRecord(
        issuer = party,
        owner = party,
        currency = currency,
        amount = LfNumeric assertFromString amount,
        observers = Vector.empty,
      )
    )

    domain.CreateCommand(TpId.Iou.Iou, arg, meta)
  }

  protected def iouExerciseTransferCommand(
      contractId: lar.ContractId
  ): domain.ExerciseCommand[v.Value, domain.EnrichedContractId] = {
    val reference = domain.EnrichedContractId(Some(TpId.Iou.Iou), contractId)
    val arg =
      recordFromFields(ShRecord(newOwner = v.Value.Sum.Party("Bob")))
    val choice = lar.Choice("Iou_Transfer")

    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
  }

  protected def iouCreateAndExerciseTransferCommand(
      partyName: domain.Party,
      amount: String = "999.9900000000",
      currency: String = "USD",
      meta: Option[domain.CommandMeta] = None,
  ): domain.CreateAndExerciseCommand[v.Record, v.Value, OptionalPkg] = {
    val party = Ref.Party assertFromString partyName.unwrap
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
      templateId = TpId.Iou.Iou,
      payload = payload,
      choice = choice,
      argument = boxedRecord(arg),
      choiceInterfaceId = None,
      meta = meta,
    )
  }

  protected def multiPartyCreateCommand(ps: List[String], value: String) = {
    val psv = lfToApi(VAx.seq(VAx.partyStr).inj(ps)).sum
    val payload = recordFromFields(
      ShRecord(
        parties = psv,
        value = v.Value.Sum.Text(value),
      )
    )
    domain.CreateCommand(
      templateId = TpId.Test.MultiPartyContract,
      payload = payload,
      meta = None,
    )
  }

  protected def multiPartyAddSignatories(cid: lar.ContractId, ps: List[String]) = {
    val psv = lfToApi(VAx.seq(VAx.partyStr).inj(ps)).sum
    val argument = boxedRecord(recordFromFields(ShRecord(newParties = psv)))
    domain.ExerciseCommand(
      reference = domain.EnrichedContractId(Some(TpId.Test.MultiPartyContract), cid),
      argument = argument,
      choiceInterfaceId = None,
      choice = lar.Choice("MPAddSignatories"),
      meta = None,
    )
  }

  protected def multiPartyFetchOther(
      cid: lar.ContractId,
      fetchedCid: lar.ContractId,
      actors: List[String],
  ) = {
    val argument = v.Value(
      v.Value.Sum.Record(
        recordFromFields(
          ShRecord(
            cid = v.Value.Sum.ContractId(fetchedCid.unwrap),
            actors = lfToApi(VAx.seq(VAx.partyStr).inj(actors)).sum,
          )
        )
      )
    )
    domain.ExerciseCommand(
      reference = domain.EnrichedContractId(Some(TpId.Test.MultiPartyContract), cid),
      argument = argument,
      choiceInterfaceId = None,
      choice = lar.Choice("MPFetchOther"),
      meta = None,
    )
  }

  protected def postContractsLookup(
      cmd: domain.ContractLocator[JsValue],
      uri: Uri,
      headers: List[HttpHeader],
      readAs: Option[List[domain.Party]],
  ): Future[(StatusCode, domain.SyncResponse[Option[domain.ActiveContract[JsValue]]])] =
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
        .parseResponse[Option[domain.ActiveContract[JsValue]]]
    } yield result

  protected def postContractsLookup(
      cmd: domain.ContractLocator[JsValue],
      uri: Uri,
      headers: List[HttpHeader],
  ): Future[(StatusCode, domain.SyncResponse[Option[domain.ActiveContract[JsValue]]])] =
    postContractsLookup(cmd, uri, headers, None)

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

  protected def assertActiveContract(uri: Uri)(
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
    jwt(uri).flatMap(jwt =>
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
    )
  }

  protected def assertActiveContract(
      activeContract: domain.ActiveContract[JsValue]
  )(
      command: domain.CreateCommand[v.Record, OptionalPkg],
      encoder: DomainJsonEncoder,
  ): Assertion = {

    import encoder.implicits._

    val expected: domain.CreateCommand[JsValue, OptionalPkg] =
      command
        .traversePayload(SprayJson.encode[v.Record](_))
        .getOrElse(fail(s"Failed to encode command: $command"))

    (activeContract.payload: JsValue) shouldBe (expected.payload: JsValue)
  }

  protected def assertTemplateId(
      actual: domain.TemplateId.RequiredPkg,
      expected: OptionalPkg,
  ): Future[Assertion] = Future {
    expected.packageId.foreach(x => actual.packageId shouldBe x)
    actual.moduleName shouldBe expected.moduleName
    actual.entityName shouldBe expected.entityName
  }

  protected def getAllPackageIds(fixture: UriFixture): Future[domain.OkResponse[List[String]]] =
    fixture
      .getRequestWithMinimumAuth[List[String]](Uri.Path("/v1/packages"))
      .map(inside(_) { case (StatusCodes.OK, x: domain.OkResponse[List[String]]) =>
        x
      })

  protected[this] def uploadPackage(fixture: UriFixture)(newDar: java.io.File): Future[Unit] = for {
    resp <- Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = fixture.uri.withPath(Uri.Path("/v1/packages")),
          headers = headersWithAdminAuth,
          entity = HttpEntity.fromFile(ContentTypes.`application/octet-stream`, newDar),
        )
      )
  } yield {
    resp.status shouldBe StatusCodes.OK
    ()
  }

  protected def initialIouCreate(
      serviceUri: Uri,
      party: domain.Party,
      headers: List[HttpHeader],
  ): Future[(StatusCode, domain.SyncResponse[domain.ActiveContract[JsValue]])] = {
    val partyJson = party.toJson.compactPrint
    val payload =
      s"""
         |{
         |  "templateId": "Iou:Iou",
         |  "payload": {
         |    "observers": [],
         |    "issuer": $partyJson,
         |    "amount": "999.99",
         |    "currency": "USD",
         |    "owner": $partyJson
         |  }
         |}
         |""".stripMargin
    HttpServiceTestFixture
      .postJsonStringRequest(
        serviceUri.withPath(Uri.Path("/v1/create")),
        payload,
        headers,
      )
      .parseResponse[domain.ActiveContract[JsValue]]
  }

  protected def initialAccountCreate(
      fixture: UriFixture with EncoderFixture,
      owner: domain.Party,
      headers: List[HttpHeader],
  ): Future[(StatusCode, domain.SyncResponse[domain.ActiveContract[JsValue]])] = {
    val command = accountCreateCommand(owner, "abc123")
    postCreateCommand(command, fixture, headers)
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
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[List[domain.ActiveContract[JsValue]]] = {
    search(commands, query, fixture, headers).map(expectOk(_))
  }

  protected def searchExpectOk(
      commands: List[domain.CreateCommand[v.Record, OptionalPkg]],
      query: JsObject,
      fixture: UriFixture with EncoderFixture,
  ): Future[List[domain.ActiveContract[JsValue]]] =
    fixture.headersWithAuth.flatMap(searchExpectOk(commands, query, fixture, _))

  protected def search(
      commands: List[domain.CreateCommand[v.Record, OptionalPkg]],
      query: JsObject,
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[
    domain.SyncResponse[List[domain.ActiveContract[JsValue]]]
  ] = {
    commands.traverse(c => postCreateCommand(c, fixture, headers)).flatMap { rs =>
      rs.map(_._1) shouldBe List.fill(commands.size)(StatusCodes.OK)
      fixture.postJsonRequest(Uri.Path("/v1/query"), query, headers).flatMap { case (_, output) =>
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
