// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.crypto.MessageDigestPrototype
import com.daml.jwt.Jwt
import com.daml.ledger.api.v2.value as v
import com.digitalasset.canton.http
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.util.ClientUtil.boxedRecord
import com.digitalasset.canton.http.util.Logging.instanceUUIDLogCtx
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.canton.ledger.api.util.LfEngineToApi
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.testing.utils.TestModels.{
  com_daml_ledger_test_ModelTestDar_path,
  com_daml_ledger_test_SemanticTestDar_path,
}
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value as lfv
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util.ByteString
import scalaz.std.scalaFuture.*
import scalaz.syntax.tag.*
import shapeless.record.Record as ShRecord
import spray.json.*

import java.security.DigestInputStream
import scala.concurrent.Future
import scala.util.Try

object AbstractHttpServiceIntegrationTestFuns {
  val dar1 = JarResourceUtils.resourceFile(com_daml_ledger_test_ModelTestDar_path)

  val dar2 = JarResourceUtils.resourceFile("Account-3.4.0.dar")

  val dar3 = JarResourceUtils.resourceFile(com_daml_ledger_test_SemanticTestDar_path)

  val userDar = JarResourceUtils.resourceFile("User-3.4.0.dar")

  val ciouDar = JarResourceUtils.resourceFile("CIou-3.4.0.dar")

  val fooV1Dar = JarResourceUtils.resourceFile("foo-0.0.1.dar")
  val fooV2Dar = JarResourceUtils.resourceFile("foo-0.0.2.dar")

  private[this] def packageIdOfDar(darFile: java.io.File): Ref.PackageId = {
    import com.digitalasset.daml.lf.{archive, typesig}
    val dar = archive.UniversalArchiveReader.assertReadFile(darFile)
    val pkgId = typesig.PackageSignature.read(dar.main)._2.packageId
    Ref.PackageId.assertFromString(pkgId)
  }

  lazy val pkgIdCiou = packageIdOfDar(ciouDar)
  lazy val pkgIdModelTests = packageIdOfDar(dar1)
  lazy val pkgIdUser = packageIdOfDar(userDar)
  lazy val pkgIdFooV1 = packageIdOfDar(fooV1Dar)
  lazy val pkgIdFooV2 = packageIdOfDar(fooV2Dar)
  lazy val pkgIdAccount = packageIdOfDar(dar2)

  lazy val pkgNameCiou = Ref.PackageName.assertFromString("CIou")
  lazy val pkgNameModelTests = Ref.PackageName.assertFromString("model-tests")
  lazy val pkgNameAccount = Ref.PackageName.assertFromString("Account")

  def packageIdToName(pkgId: Ref.PackageId): Ref.PackageName = pkgId match {
    case id if id == pkgIdCiou => pkgNameCiou
    case id if id == pkgIdModelTests => pkgNameModelTests
    case id if id == pkgIdAccount => pkgNameAccount
    case _ => throw new IllegalArgumentException(s"Unexpected package id: $pkgId")
  }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
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
  object VAx {
    def seq(elem: VA): VA.Aux[Seq[elem.Inj]] =
      VA.list(elem).xmap((xs: Seq[elem.Inj]) => xs)(_.toVector)

    // nest assertFromString into arbitrary VA structures
    val partyStr: VA.Aux[String] = VA.party.xmap(identity[String])(Ref.Party.assertFromString)

    val partySynchronizer: VA.Aux[http.Party] = http.Party.subst[VA.Aux, String](partyStr)

    val contractIdSynchronizer: VA.Aux[http.ContractId] = {
      import lfv.test.ValueGenerators.coidGen
      import org.scalacheck.Arbitrary
      implicit val arbCid: Arbitrary[lfv.Value.ContractId] = Arbitrary(coidGen)
      http.ContractId subst VA.contractId.xmap(_.coid: String)(
        lfv.Value.ContractId.fromString(_).fold(sys.error, identity)
      )
    }
  }

  trait UriFixture {
    def uri: Uri
  }
  trait EncoderFixture {
    def encoder: ApiJsonEncoder
  }
  sealed trait DecoderFixture {
    def decoder: ApiJsonDecoder
  }

  final case class HttpServiceOnlyTestFixtureData(
      uri: Uri,
      encoder: ApiJsonEncoder,
      decoder: ApiJsonDecoder,
  ) extends UriFixture
      with EncoderFixture
      with DecoderFixture

  final case class HttpServiceTestFixtureData(
      uri: Uri,
      encoder: ApiJsonEncoder,
      decoder: ApiJsonDecoder,
      client: DamlLedgerClient,
  ) extends UriFixture
      with EncoderFixture
      with DecoderFixture
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait AbstractHttpServiceIntegrationTestFuns extends HttpJsonApiTestBase with HttpTestFuns {
  import AbstractHttpServiceIntegrationTestFuns.*
  import JsonProtocol.*

  override def packageFiles = List(dar1, dar2, userDar)

  protected def removeRecordId(a: v.Value): v.Value = a match {
    case v.Value(v.Value.Sum.Record(r)) if r.recordId.isDefined =>
      v.Value(v.Value.Sum.Record(removeRecordId(r)))
    case _ =>
      a
  }

  protected def removeRecordId(a: v.Record): v.Record = a.copy(recordId = None)

  import com.digitalasset.daml.lf.data.Numeric as LfNumeric
  import shapeless.HList

  private[this] object RecordFromFields extends shapeless.Poly1 {
    import shapeless.Witness
    import shapeless.labelled.FieldType as :->>:

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
    import com.digitalasset.daml.lf.data.Numeric.Scale
    val iouT = ShRecord(
      issuer = VAx.partySynchronizer,
      owner = VAx.partySynchronizer,
      currency = VA.text,
      amount = VA.numeric(Scale assertFromInt 10),
      observers = VA.list(VAx.partySynchronizer),
    )
    VA.record(Ref.Identifier assertFromString "none:Iou:Iou", iouT)
  }

  protected[this] object TpId {
    import com.digitalasset.canton.http.ContractTypeId as CtId
    import CtId.Interface.RequiredPkg as IId
    import CtId.Template.RequiredPkg as TId
    import Ref.PackageRef.Id as PkgId

    object Iou {
      val Dummy: TId = CtId.Template(PkgId(pkgIdModelTests), "Iou", "Dummy")
      val IIou: IId = CtId.Interface(PkgId(pkgIdModelTests), "Iou", "IIou")
      val Iou: TId = CtId.Template(PkgId(pkgIdModelTests), "Iou", "Iou")
      val IouTransfer: TId = CtId.Template(PkgId(pkgIdModelTests), "Iou", "IouTransfer")
    }
    object Test {
      val Dummy: TId = CtId.Template(PkgId(pkgIdModelTests), "Test", "Dummy")
      val MultiPartyContract: TId =
        CtId.Template(PkgId(pkgIdModelTests), "Test", "MultiPartyContract")
    }
    object Account {
      val Account: TId = CtId.Template(PkgId(pkgIdAccount), "Account", "Account")
      val Helper: TId = CtId.Template(PkgId(pkgIdAccount), "Account", "Helper")
      val IAccount: IId = CtId.Interface(PkgId(pkgIdAccount), "Account", "IAccount")
      val KeyedByDecimal: IId = CtId.Interface(PkgId(pkgIdAccount), "Account", "KeyedByDecimal")
      val KeyedByVariantAndRecord: TId =
        CtId.Template(PkgId(pkgIdAccount), "Account", "KeyedByVariantAndRecord")
      val PubSub: TId = CtId.Template(PkgId(pkgIdAccount), "Account", "PubSub")
      val SharedAccount: TId = CtId.Template(PkgId(pkgIdAccount), "Account", "SharedAccount")
    }
    object Disclosure {
      val AnotherToDisclose: TId =
        CtId.Template(PkgId(pkgIdAccount), "Disclosure", "AnotherToDisclose")
      val ToDisclose: TId = CtId.Template(PkgId(pkgIdAccount), "Disclosure", "ToDisclose")
      val HasGarbage: IId = CtId.Interface(
        Ref.PackageRef.Name(Ref.PackageName.assertFromString("Account")),
        "Disclosure",
        "HasGarbage",
      )
      val Viewport: TId = CtId.Template(PkgId(pkgIdAccount), "Disclosure", "Viewport")
      val CheckVisibility: TId = CtId.Template(PkgId(pkgIdAccount), "Disclosure", "CheckVisibility")
    }
    object User {
      val User: TId = CtId.Template(PkgId(pkgIdUser), "User", "User")
    }
    object CIou {
      val CIou: TId = CtId.Template(PkgId(pkgIdCiou), "CIou", "CIou")
    }
    object IIou {
      val IIou: IId = CtId.Interface(PkgId(pkgIdCiou), "IIou", "IIou")
      val TestIIou: TId = CtId.Template(PkgId(pkgIdCiou), "IIou", "TestIIou")
    }
    object Transferrable {
      val Transferrable: IId = CtId.Interface(PkgId(pkgIdCiou), "Transferrable", "Transferrable")
    }
  }

  protected def iouCreateCommand(
      party: http.Party,
      amount: String = "999.9900000000",
      currency: String = "USD",
      observers: Vector[http.Party] = Vector.empty,
      meta: Option[http.CommandMeta.NoDisclosed] = None,
  ): http.CreateCommand[v.Record, http.ContractTypeId.Template.RequiredPkg] = {
    val arg = argToApi(iouVA)(
      ShRecord(
        issuer = party,
        owner = party,
        currency = currency,
        amount = LfNumeric assertFromString amount,
        observers = observers,
      )
    )

    http.CreateCommand(TpId.Iou.Iou, arg, meta)
  }

  protected def iouExerciseTransferCommand(
      contractId: lar.ContractId,
      partyName: http.Party,
  ): http.ExerciseCommand[Nothing, v.Value, http.EnrichedContractId] = {
    val reference = http.EnrichedContractId(Some(TpId.Iou.Iou), contractId)
    val party = Ref.Party assertFromString partyName.unwrap
    val arg =
      recordFromFields(ShRecord(newOwner = v.Value.Sum.Party(party)))
    val choice = lar.Choice("Iou_Transfer")

    http.ExerciseCommand(reference, choice, boxedRecord(arg), None, None)
  }

  protected def encodeExercise(encoder: ApiJsonEncoder)(
      exercise: http.ExerciseCommand.RequiredPkg[v.Value, http.ContractLocator[v.Value]]
  ): JsValue =
    encoder.encodeExerciseCommand(exercise).getOrElse(fail(s"Cannot encode: $exercise"))

  protected def decodeExercise(
      decoder: ApiJsonDecoder,
      jwt: Jwt,
  )(
      jsVal: JsValue
  ): Future[http.ExerciseCommand.RequiredPkg[v.Value, http.EnrichedContractId]] =
    instanceUUIDLogCtx { implicit lc =>
      import scalaz.syntax.bifunctor.*
      val cmd =
        decoder.decodeExerciseCommand(jsVal, jwt).getOrElse(fail(s"Cannot decode $jsVal"))
      cmd.map(
        _.bimap(
          lfToApi,
          enrichedContractIdOnly,
        )
      )
    }

  protected def enrichedContractIdOnly(x: http.ContractLocator[_]): http.EnrichedContractId =
    x match {
      case cid: http.EnrichedContractId => cid
      case _: http.EnrichedContractKey[_] =>
        fail(s"Expected synchronizer.EnrichedContractId, got: $x")
    }

  protected def lfToApi(lfVal: http.LfValue): v.Value =
    LfEngineToApi.lfValueToApiValue(verbose = true, lfVal).fold(e => fail(e), identity)

}
