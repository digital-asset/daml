// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api

import java.security.cert.X509Certificate
import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.{Get, Put}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import com.daml.nonrepudiation.api.v1.{CertificatesEndpoint, SignedPayloadsEndpoint}
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
import com.daml.nonrepudiation.testing._
import com.daml.ports.FreePort
import com.google.common.io.BaseEncoding
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, OptionValues}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Random

final class NonRepudiationApiSpec
    extends AsyncFlatSpec
    with Matchers
    with OptionValues
    with BeforeAndAfterAll
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
        payloadsAfterFirstAdd <- Unmarshal(secondResponse).to[List[SignedPayloadsEndpoint.Response]]
        _ = signedPayloads.put(toSignedPayload(secondExpectedResponse))
        thirdResponse <- getSignedPayload(baseUrl, commandId)
        _ = thirdResponse.status.intValue shouldBe 200
        payloadsAfterSecondAdd <- Unmarshal(thirdResponse).to[List[SignedPayloadsEndpoint.Response]]
      } yield {
        payloadsAfterFirstAdd should contain only firstExpectedResponse
        payloadsAfterSecondAdd should contain.allOf(firstExpectedResponse, secondExpectedResponse)
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
        secondResponseBody <- Unmarshal(secondResponse).to[CertificatesEndpoint.Certificate]
      } yield {
        val certificate = BaseEncoding.base64Url().decode(secondResponseBody.certificate)
        certificate shouldEqual expectedCertificate.getEncoded
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
        responseBody <- Unmarshal(response).to[CertificatesEndpoint.Fingerprint]
        _ = responseBody.fingerprint shouldBe expectedFingerprint
      } yield {
        val certificate = certificates.get(fingerprintBytes).value
        certificate.getEncoded shouldEqual expectedCertificate.getEncoded
      }
  }

  it should "return the expected error if the fingerprint string is malformed" in withApi() {
    (baseUrl, _, _) =>
      for {
        response <- getCertificate(baseUrl, "not-a-fingerprint")
        (status, body) <- statusAndBody(response)
      } yield {
        status shouldBe 400
        body shouldBe CertificatesEndpoint.InvalidFingerprintString
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
        (status, body) <- statusAndBody(response)
      } yield {
        status shouldBe 400
        body shouldBe CertificatesEndpoint.InvalidCertificateString
      }
  }

  it should "return the expected error if the certificate is invalid" in withApi() {
    (baseUrl, _, _) =>
      val randomBytes = Array.fill(2048)(Random.nextInt.toByte)
      val malformedCertificate = BaseEncoding.base64Url().encode(randomBytes)
      for {
        response <- putCertificate(baseUrl, s"""{"certificate":"$malformedCertificate"}""")
        (status, body) <- statusAndBody(response)
      } yield {
        status shouldBe 400
        body shouldBe CertificatesEndpoint.InvalidCertificateFormat
      }
  }

  it should "return the expected error if the backend fails" in withApi(certificates =
    NonRepudiationApiSpec.BrokenCertificates
  ) { (baseUrl, _, _) =>
    val (_, expectedCertificate) = generateKeyAndCertificate()

    for {
      response <- putCertificate(baseUrl, expectedCertificate)
      (status, body) <- statusAndBody(response)
    } yield {
      status shouldBe 500
      body shouldBe CertificatesEndpoint.UnableToAddTheCertificate
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

    val configuration = NonRepudiationApi.Configuration.Default.copy(port = port)

    val api =
      NonRepudiationApi.owner(
        configuration,
        certificates,
        signedPayloads,
      )

    val baseUrl = s"http://${configuration.interface}:${configuration.port}"

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

  private def statusAndBody(response: HttpResponse): Future[(Int, String)] =
    response.entity
      .getDataBytes()
      .runWith(Sink.seq[ByteString], actorSystem)
      .map(_.reduce(_ ++ _))
      .map(bytes => response.status.intValue -> bytes.utf8String)

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

  def toSignedPayload(response: SignedPayloadsEndpoint.Response): SignedPayload =
    SignedPayload(
      algorithm = AlgorithmString.wrap(response.algorithm),
      fingerprint = FingerprintBytes.wrap(BaseEncoding.base64Url().decode(response.fingerprint)),
      payload = PayloadBytes.wrap(BaseEncoding.base64Url().decode(response.payload)),
      signature = SignatureBytes.wrap(BaseEncoding.base64Url().decode(response.signature)),
      timestamp = Instant.ofEpochMilli(response.timestamp),
    )

}
