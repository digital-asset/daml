// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  private[http] val riouDar = requiredResource("ledger-service/http-json/RIou.dar")

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

    val contractIdDomain: VA.Aux[domain.ContractId] = {
      import org.scalacheck.Arbitrary, lfv.test.ValueGenerators.coidGen
      implicit val arbCid: Arbitrary[lfv.Value.ContractId] = Arbitrary(coidGen)
      domain.ContractId subst VA.contractId.xmap(_.coid: String)(
        lfv.Value.ContractId.fromString(_).fold(sys.error, identity)
      )
    }
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

  lazy protected val metadata2: MetadataReader.LfMetadata =
    MetadataReader.readFromDar(dar2).valueOr(e => fail(s"Cannot read dar2 metadata: $e"))

  lazy protected val metadataUser: MetadataReader.LfMetadata =
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
    ): Future[(domain.Party, List[HttpHeader])] =
      self.getUniquePartyTokenAndAuthHeaders(name).map { case (p, _, h) => (p, h) }

    def getUniquePartyTokenAndAuthHeaders(
        name: String
    ): Future[(domain.Party, Jwt, List[HttpHeader])] = {
      val party = getUniqueParty(name)
      for {
        jwt <- jwtForParties(uri)(List(party), List.empty, "", false, false)
        headers = authorizationHeader(jwt)
        request = domain.AllocatePartyRequest(
          Some(party),
          None,
        )
        json = SprayJson.encode(request).valueOr(e => fail(e.shows))
        _ <- postJsonRequest(
          Uri.Path("/v1/parties/allocate"),
          json = json,
          headers = headersWithAdminAuth,
        )
      } yield (party, jwt, headers)
    }

    def headersWithAuth(implicit ec: ExecutionContext): Future[List[Authorization]] =
      jwt(uri)(ec).map(authorizationHeader)

    def headersWithPartyAuth(
        actAs: List[domain.Party],
        readAs: List[domain.Party] = List.empty,
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
    ): Future[domain.SyncResponse[Result]] =
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
    ): Future[domain.SyncResponse[Result]] =
      headersWithAuth
        .flatMap(getRequest(path, _))
        .parseResponse[Result]
  }

  implicit protected final class `Future JsValue functions`(
      private val self: Future[(StatusCode, JsValue)]
  ) {
    def parseResponse[Result: JsonReader]: Future[domain.SyncResponse[Result]] =
      self.map { case (status, jsv) =>
        val r = decode1[domain.SyncResponse, Result](jsv).fold(e => fail(e.shows), identity)
        r.status should ===(status)
        r
      }
  }

  protected def postCreateCommand(
      cmd: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg],
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[domain.SyncResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]] =
    HttpServiceTestFixture
      .postCreateCommand(cmd, fixture.encoder, fixture.uri, headers)
      .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]

  protected def postCreateCommand(
      cmd: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg],
      fixture: UriFixture with EncoderFixture,
  ): Future[domain.SyncResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]] =
    fixture.headersWithAuth.flatMap(postCreateCommand(cmd, fixture, _))

  protected def resultContractId(
      r: domain.SyncResponse[domain.ActiveContract[_, _]]
  ) =
    inside(r) { case domain.OkResponse(result, _, _: StatusCodes.Success) =>
      result.contractId
    }

  protected def postArchiveCommand(
      templateId: domain.ContractTypeId.OptionalPkg,
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
      templateId: domain.ContractTypeId.OptionalPkg,
      contractId: domain.ContractId,
      fixture: UriFixture with EncoderFixture,
  ): Future[(StatusCode, JsValue)] =
    fixture.headersWithAuth.flatMap(
      postArchiveCommand(templateId, contractId, fixture, _)
    )

  protected def lookupContractAndAssert(
      contractLocator: domain.ContractLocator[JsValue],
      contractId: ContractId,
      create: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg],
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[Assertion] =
    postContractsLookup(contractLocator, fixture.uri, headers).map(inside(_) {
      case domain.OkResponse(Some(resultContract), _, StatusCodes.OK) =>
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

  protected def removePackageId(
      tmplId: domain.ContractTypeId.RequiredPkg
  ): domain.ContractTypeId.OptionalPkg =
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
      issuer = VAx.partyDomain,
      owner = VAx.partyDomain,
      currency = VA.text,
      amount = VA.numeric(Scale assertFromInt 10),
      observers = VA.list(VAx.partyDomain),
    )
    VA.record(Ref.Identifier assertFromString "none:Iou:Iou", iouT)
  }

  protected[this] object TpId {
    import domain.{ContractTypeId => CtId}
    import CtId.Template.{OptionalPkg => TId}
    import CtId.Interface.{OptionalPkg => IId}

    object Iou {
      val Iou: TId = CtId.Template(None, "Iou", "Iou")
      val IouTransfer: TId = CtId.Template(None, "Iou", "IouTransfer")
    }
    object Test {
      val MultiPartyContract: TId = CtId.Template(None, "Test", "MultiPartyContract")
    }
    object IAccount {
      val IAccount: IId = CtId.Interface(None, "IAccount", "IAccount")
    }
    object Account {
      val Account: TId = CtId.Template(None, "Account", "Account")
      val KeyedByVariantAndRecord: TId = CtId.Template(None, "Account", "KeyedByVariantAndRecord")
    }
    object Disclosure {
      val ToDisclose: TId = CtId.Template(None, "Disclosure", "ToDisclose")
      val Viewport: TId = CtId.Template(None, "Disclosure", "Viewport")
    }
    object User {
      val User: TId = CtId.Template(None, "User", "User")
    }
    object IIou {
      val IIou: IId = CtId.Interface(None, "IIou", "IIou")
    }
    object RIou {
      val RIou: IId = CtId.Interface(None, "RIou", "RIou")
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
      party: domain.Party,
      amount: String = "999.9900000000",
      currency: String = "USD",
      observers: Vector[domain.Party] = Vector.empty,
      meta: Option[domain.CommandMeta.NoDisclosed] = None,
  ): domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg] = {
    val arg = argToApi(iouVA)(
      ShRecord(
        issuer = party,
        owner = party,
        currency = currency,
        amount = LfNumeric assertFromString amount,
        observers = observers,
      )
    )

    domain.CreateCommand(TpId.Iou.Iou, arg, meta)
  }

  private[this] val (_, ciouVA) = {
    val iouT = ShRecord(issuer = VAx.partyDomain, owner = VAx.partyDomain, amount = VA.text)
    VA.record(Ref.Identifier assertFromString "none:Iou:Iou", iouT)
  }

  protected def iouCommand(
      issuer: domain.Party,
      templateId: domain.ContractTypeId.Template.OptionalPkg,
  ) = {
    val iouT = argToApi(ciouVA)(
      ShRecord(
        issuer = issuer,
        owner = issuer,
        amount = "42",
      )
    )
    domain.CreateCommand(templateId, iouT, None)
  }

  protected def iouExerciseTransferCommand(
      contractId: lar.ContractId,
      partyName: domain.Party,
  ): domain.ExerciseCommand[Nothing, v.Value, domain.EnrichedContractId] = {
    val reference = domain.EnrichedContractId(Some(TpId.Iou.Iou), contractId)
    val party = Ref.Party assertFromString partyName.unwrap
    val arg =
      recordFromFields(ShRecord(newOwner = v.Value.Sum.Party(party)))
    val choice = lar.Choice("Iou_Transfer")

    domain.ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
  }

  protected def iouCreateAndExerciseTransferCommand(
      originator: domain.Party,
      target: domain.Party,
      amount: String = "999.9900000000",
      currency: String = "USD",
      meta: Option[domain.CommandMeta.NoDisclosed] = None,
  ): domain.CreateAndExerciseCommand[
    v.Record,
    v.Value,
    domain.ContractTypeId.Template.OptionalPkg,
    domain.ContractTypeId.OptionalPkg,
  ] = {
    val targetParty = Ref.Party assertFromString target.unwrap
    val payload = argToApi(iouVA)(
      ShRecord(
        issuer = originator,
        owner = originator,
        currency = currency,
        amount = LfNumeric assertFromString amount,
        observers = Vector.empty,
      )
    )

    val arg =
      recordFromFields(ShRecord(newOwner = v.Value.Sum.Party(targetParty)))
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

  protected def multiPartyCreateCommand(ps: List[domain.Party], value: String) = {
    val psv = lfToApi(VAx.seq(VAx.partyDomain).inj(ps)).sum
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

  protected def multiPartyAddSignatories(cid: lar.ContractId, ps: List[domain.Party]) = {
    val psv = lfToApi(VAx.seq(VAx.partyDomain).inj(ps)).sum
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
      actors: List[domain.Party],
  ) = {
    val argument = v.Value(
      v.Value.Sum.Record(
        recordFromFields(
          ShRecord(
            cid = v.Value.Sum.ContractId(fetchedCid.unwrap),
            actors = lfToApi(VAx.seq(VAx.partyDomain).inj(actors)).sum,
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
  ): Future[domain.SyncResponse[Option[domain.ActiveContract.ResolvedCtTyId[JsValue]]]] =
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
        .parseResponse[Option[domain.ActiveContract.ResolvedCtTyId[JsValue]]]
    } yield result

  protected def postContractsLookup(
      cmd: domain.ContractLocator[JsValue],
      uri: Uri,
      headers: List[HttpHeader],
  ): Future[domain.SyncResponse[Option[domain.ActiveContract.ResolvedCtTyId[JsValue]]]] =
    postContractsLookup(cmd, uri, headers, None)

  protected def asContractId(a: JsValue): domain.ContractId = inside(a) { case JsString(x) =>
    domain.ContractId(x)
  }

  protected def encodeExercise(encoder: DomainJsonEncoder)(
      exercise: domain.ExerciseCommand.OptionalPkg[v.Value, domain.ContractLocator[v.Value]]
  ): JsValue =
    encoder.encodeExerciseCommand(exercise).getOrElse(fail(s"Cannot encode: $exercise"))

  protected def decodeExercise(
      decoder: DomainJsonDecoder,
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(
      jsVal: JsValue
  ): Future[domain.ExerciseCommand.RequiredPkg[v.Value, domain.EnrichedContractId]] =
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
      actual: domain.ActiveContract.ResolvedCtTyId[JsValue],
      create: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg],
      exercise: domain.ExerciseCommand[Any, v.Value, _],
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
      activeContract: domain.ActiveContract.ResolvedCtTyId[JsValue]
  )(
      command: domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg],
      encoder: DomainJsonEncoder,
  ): Assertion = {

    import encoder.implicits._

    val expected: domain.CreateCommand[JsValue, domain.ContractTypeId.Template.OptionalPkg] =
      command
        .traversePayload(SprayJson.encode[v.Record](_))
        .getOrElse(fail(s"Failed to encode command: $command"))

    (activeContract.payload: JsValue) shouldBe (expected.payload: JsValue)
  }

  protected def assertJsPayload(
      activeContract: domain.ActiveContract.ResolvedCtTyId[JsValue]
  )(
      jsPayload: JsValue
  ): Assertion = {
    (activeContract.payload: JsValue) shouldBe (jsPayload)
  }

  protected def assertTemplateId(
      actual: domain.ContractTypeId.RequiredPkg,
      expected: domain.ContractTypeId.OptionalPkg,
  ): Future[Assertion] = Future {
    expected.packageId.foreach(x => actual.packageId shouldBe x)
    actual.moduleName shouldBe expected.moduleName
    actual.entityName shouldBe expected.entityName
  }

  protected def getAllPackageIds(fixture: UriFixture): Future[domain.OkResponse[List[String]]] =
    fixture
      .getRequestWithMinimumAuth[List[String]](Uri.Path("/v1/packages"))
      .map(inside(_) { case x @ domain.OkResponse(_, _, StatusCodes.OK) =>
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
  ): Future[domain.SyncResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]] = {
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
      .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
  }

  protected def initialAccountCreate(
      fixture: UriFixture with EncoderFixture,
      owner: domain.Party,
      headers: List[HttpHeader],
  ): Future[domain.SyncResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]] = {
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
      commands: List[domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg]],
      query: JsObject,
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]] = {
    search(commands, query, fixture, headers).map(expectOk(_))
  }

  protected def searchExpectOk(
      commands: List[domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg]],
      query: JsObject,
      fixture: UriFixture with EncoderFixture,
  ): Future[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]] =
    fixture.headersWithAuth.flatMap(searchExpectOk(commands, query, fixture, _))

  protected def search(
      commands: List[domain.CreateCommand[v.Record, domain.ContractTypeId.Template.OptionalPkg]],
      query: JsObject,
      fixture: UriFixture with EncoderFixture,
      headers: List[HttpHeader],
  ): Future[
    domain.SyncResponse[List[domain.ActiveContract.ResolvedCtTyId[JsValue]]]
  ] = {
    commands.traverse(c => postCreateCommand(c, fixture, headers)).flatMap { rs =>
      rs.map(_.status) shouldBe List.fill(commands.size)(StatusCodes.OK)
      fixture.postJsonRequest(Uri.Path("/v1/query"), query, headers).flatMap { case (_, output) =>
        FutureUtil
          .toFuture(
            decode1[domain.SyncResponse, List[domain.ActiveContract.ResolvedCtTyId[JsValue]]](
              output
            )
          )
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
