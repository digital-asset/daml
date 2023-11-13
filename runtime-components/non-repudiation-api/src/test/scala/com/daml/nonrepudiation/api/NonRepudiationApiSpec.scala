// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api

import java.net.{InetAddress, InetSocketAddress}
import java.security.cert.X509Certificate
import java.time.Instant
import java.util.UUID

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.client.RequestBuilding.{Get, Put}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import com.daml.nonrepudiation.api.v1.{CertificatesEndpoint, SignedPayloadsEndpoint}
import com.daml.nonrepudiation.testing._
import com.daml.nonrepudiation.{
  AlgorithmString,
  CertificateRepository,
  CommandIdString,
  FingerprintBytes,
  PayloadBytes,
  SignatureBytes,
  SignedPayload,
  SignedPayloadRepository,
}
import com.daml.ports.FreePort
import com.google.common.io.BaseEncoding
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, OptionValues}
import spray.json.JsonFormat

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Random

final class NonRepudiationApiSpec
    extends AsyncFlatSpec
    with Matchers
    with OptionValues
    with BeforeAndAfterAll
    with Result.JsonProtocol
    with SignedPayloadsEndpoint.JsonProtocol
    with CertificatesEndpoint.JsonProtocol {

  import NonRepudiationApiSpec._

  private implicit val actorSystem: ActorSystem = ActorSystem("non-repudiation-api-test")

  behavior of "NonRepudiationApi"

  it should "correctly retrieve saved payloads by identifier" in withApi() {
    (baseUrl, _, signedPayloads) =>
      val commandId = UUID.randomUUID.toString

      val firstSignedPayload =
        SignedPayload(
          algorithm = AlgorithmString.wrap("SuperCryptoStuff256"),
          fingerprint = FingerprintBytes.wrap("fingerprint-1".getBytes),
          payload = PayloadBytes.wrap(generateCommand(commandId = commandId).toByteArray),
          signature = SignatureBytes.wrap("signature-1".getBytes),
          timestamp = Instant.ofEpochMilli(42),
        )

      val firstExpectedResponse = SignedPayloadsEndpoint.toResponse(firstSignedPayload)

      val secondSignedPayload =
        SignedPayload(
          algorithm = AlgorithmString.wrap("SuperCryptoStuff512"),
          fingerprint = FingerprintBytes.wrap("fingerprint-2".getBytes),
          payload = PayloadBytes.wrap(generateCommand(commandId = commandId).toByteArray),
          signature = SignatureBytes.wrap("signature-2".getBytes),
          timestamp = Instant.ofEpochMilli(47),
        )

      val secondExpectedResponse = SignedPayloadsEndpoint.toResponse(secondSignedPayload)

      for {
        firstResponse <- getSignedPayload(baseUrl, commandId)
        _ = firstResponse.status.intValue shouldBe 404
        _ = signedPayloads.put(toSignedPayload(firstExpectedResponse))
        secondResponse <- getSignedPayload(baseUrl, commandId)
        _ = secondResponse.status.intValue shouldBe 200
        payloadsAfterFirstAdd <- expect[List[SignedPayloadsEndpoint.Response]](secondResponse)
        _ = signedPayloads.put(toSignedPayload(secondExpectedResponse))
        thirdResponse <- getSignedPayload(baseUrl, commandId)
        _ = thirdResponse.status.intValue shouldBe 200
        payloadsAfterSecondAdd <- expect[List[SignedPayloadsEndpoint.Response]](thirdResponse)
      } yield {
        payloadsAfterFirstAdd.result should contain only firstExpectedResponse
        payloadsAfterSecondAdd.result should contain.allOf(
          firstExpectedResponse,
          secondExpectedResponse,
        )
      }

  }

  it should "return the expected error if a signed payload cannot be retrieved" in withApi(
    signedPayloads = NonRepudiationApiSpec.BrokenSignedPayloads
  ) { (baseUrl, _, _) =>
    for {
      response <- getSignedPayload(baseUrl, "not-really-important")
      failure <- Unmarshal(response).to[Result.Failure]
    } yield {
      response.status.intValue shouldBe 500
      failure.status shouldBe 500
      failure.error shouldBe SignedPayloadsEndpoint.UnableToRetrieveTheSignedPayload
    }
  }

  it should "correctly show a certificate in the backend" in withApi() {
    (baseUrl, certificates, _) =>
      val (_, expectedCertificate) = generateKeyAndCertificate()
      val fingerprintBytes = FingerprintBytes.compute(expectedCertificate)
      val fingerprint = BaseEncoding.base64Url().encode(fingerprintBytes.unsafeArray)

      for {
        firstResponse <- getCertificate(baseUrl, fingerprint)
        _ = firstResponse.status.intValue shouldBe 404
        expectedFingerprint = certificates.put(expectedCertificate)
        _ = fingerprintBytes shouldEqual expectedFingerprint
        secondResponse <- getCertificate(baseUrl, fingerprint)
        secondResponseBody <- expect[CertificatesEndpoint.Certificate](secondResponse)
      } yield {
        val encodedCertificate = secondResponseBody.result.certificate
        val certificateBytes = BaseEncoding.base64Url().decode(encodedCertificate)
        certificateBytes shouldEqual expectedCertificate.getEncoded
      }
  }

  it should "correctly upload a certificate to the backend" in withApi() {
    (baseUrl, certificates, _) =>
      val (_, expectedCertificate) = generateKeyAndCertificate()
      val fingerprintBytes = FingerprintBytes.compute(expectedCertificate)
      val expectedFingerprint = BaseEncoding.base64Url().encode(fingerprintBytes.unsafeArray)

      certificates.get(fingerprintBytes) shouldBe empty

      for {
        response <- putCertificate(baseUrl, expectedCertificate)
        _ = response.status.intValue shouldBe 200
        responseBody <- expect[CertificatesEndpoint.Fingerprint](response)
        _ = responseBody.result.fingerprint shouldBe expectedFingerprint
      } yield {
        val certificate = certificates.get(fingerprintBytes).value
        certificate.getEncoded shouldEqual expectedCertificate.getEncoded
      }
  }

  it should "return the expected error if the fingerprint string is malformed" in withApi() {
    (baseUrl, _, _) =>
      for {
        response <- getCertificate(baseUrl, "not-a-fingerprint")
        result <- Unmarshal(response).to[Result.Failure]
      } yield {
        response.status.intValue shouldBe 400
        result.status shouldBe 400
        result.error shouldBe CertificatesEndpoint.InvalidFingerprintString
      }
  }

  it should "return 400 if the request is malformed" in withApi() { (baseUrl, _, _) =>
    for {
      response <- putCertificate(baseUrl, """{"foobar":42}""")
    } yield response.status.intValue shouldBe 400
  }

  it should "return the expected error if the certificate string is malformed" in withApi() {
    (baseUrl, _, _) =>
      for {
        response <- putCertificate(baseUrl, """{"certificate":"Ceci n'est pas un certificat"}""")
        result <- Unmarshal(response).to[Result.Failure]
      } yield {
        response.status.intValue shouldBe 400
        result.status shouldBe 400
        result.error shouldBe CertificatesEndpoint.InvalidCertificateString
      }
  }

  it should "return the expected error if the certificate is invalid" in withApi() {
    (baseUrl, _, _) =>
      val randomBytes = Array.fill(2048)(Random.nextInt().toByte)
      val malformedCertificate = BaseEncoding.base64Url().encode(randomBytes)
      for {
        response <- putCertificate(baseUrl, s"""{"certificate":"$malformedCertificate"}""")
        result <- Unmarshal(response).to[Result.Failure]
      } yield {
        response.status.intValue shouldBe 400
        result.status shouldBe 400
        result.error shouldBe CertificatesEndpoint.InvalidCertificateFormat
      }
  }

  it should "return the expected error if a certificate cannot be added" in withApi(certificates =
    NonRepudiationApiSpec.BrokenCertificates
  ) { (baseUrl, _, _) =>
    val (_, expectedCertificate) = generateKeyAndCertificate()
    val fingerprintBytes = FingerprintBytes.compute(expectedCertificate)
    val expectedFingerprint = BaseEncoding.base64Url().encode(fingerprintBytes.unsafeArray)

    for {
      response <- getCertificate(baseUrl, expectedFingerprint)
      result <- Unmarshal(response).to[Result.Failure]
    } yield {
      response.status.intValue shouldBe 500
      result.status shouldBe 500
      result.error shouldBe CertificatesEndpoint.UnableToRetrieveTheCertificate
    }
  }

  it should "return the expected error if a certificated cannot be retrieved" in withApi(
    certificates = NonRepudiationApiSpec.BrokenCertificates
  ) { (baseUrl, _, _) =>
    val (_, expectedCertificate) = generateKeyAndCertificate()

    for {
      response <- putCertificate(baseUrl, expectedCertificate)
      result <- Unmarshal(response).to[Result.Failure]
    } yield {
      response.status.intValue shouldBe 500
      result.status shouldBe 500
      result.error shouldBe CertificatesEndpoint.UnableToAddTheCertificate
    }
  }

  def withApi(
      certificates: CertificateRepository = new Certificates,
      signedPayloads: SignedPayloadRepository[CommandIdString] = new SignedPayloads[CommandIdString],
  )(
      test: (
          String,
          CertificateRepository,
          SignedPayloadRepository[CommandIdString],
      ) => Future[Assertion]
  ): Future[Assertion] = {

    val port = FreePort.find().value

    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, port)

    val api =
      NonRepudiationApi.owner(
        address = address,
        shutdownTimeout = 10.seconds,
        certificates,
        signedPayloads,
        actorSystem,
      )

    val baseUrl = s"http://${address.getAddress.getHostAddress}:${address.getPort}"

    api.use { _ => test(baseUrl, certificates, signedPayloads) }

  }

  private def getSignedPayload(baseUrl: String, commandId: String): Future[HttpResponse] =
    Http().singleRequest(Get(s"$baseUrl/v1/command/$commandId"))

  private def getCertificate(baseUrl: String, fingerprint: String): Future[HttpResponse] =
    Http().singleRequest(Get(s"$baseUrl/v1/certificate/$fingerprint"))

  private def putCertificate(
      baseUrl: String,
      certificate: X509Certificate,
  ): Future[HttpResponse] = {
    val encodedCertificate = BaseEncoding.base64Url().encode(certificate.getEncoded)
    val request = CertificatesEndpoint.Certificate(encodedCertificate)
    putCertificate(baseUrl, certificateFormat.write(request).toString)
  }

  private def putCertificate(baseUrl: String, string: String): Future[HttpResponse] = {
    val entity = HttpEntity(contentType = ContentTypes.`application/json`, string)
    Http().singleRequest(Put(s"$baseUrl/v1/certificate").withEntity(entity))
  }

  def expect[A: JsonFormat](response: HttpResponse): Future[Result.Success[A]] =
    Unmarshal(response).to[Result.Success[A]]

  override protected def afterAll(): Unit = {
    super.afterAll()
    val _ = Await.result(actorSystem.terminate(), 5.seconds)
  }

}

object NonRepudiationApiSpec {

  private object BrokenCertificates extends CertificateRepository {

    override def get(fingerprint: FingerprintBytes): Option[X509Certificate] =
      throw new UnsupportedOperationException("The certificate repository is broken")

    override def put(certificate: X509Certificate): FingerprintBytes =
      throw new UnsupportedOperationException("The certificate repository is broken")

  }

  private object BrokenSignedPayloads extends SignedPayloadRepository[CommandIdString] {

    override def get(key: CommandIdString): Iterable[SignedPayload] =
      throw new UnsupportedOperationException("The signed payload repository is broken")

    override def put(signedPayload: SignedPayload): Unit =
      throw new UnsupportedOperationException("The signed payload repository is broken")

  }

  def toSignedPayload(response: SignedPayloadsEndpoint.Response): SignedPayload =
    SignedPayload(
      algorithm = AlgorithmString.wrap(response.algorithm),
      fingerprint = FingerprintBytes.wrap(BaseEncoding.base64Url().decode(response.fingerprint)),
      payload = PayloadBytes.wrap(BaseEncoding.base64Url().decode(response.payload)),
      signature = SignatureBytes.wrap(BaseEncoding.base64Url().decode(response.signature)),
      timestamp = Instant.ofEpochMilli(response.timestamp),
    )

}
