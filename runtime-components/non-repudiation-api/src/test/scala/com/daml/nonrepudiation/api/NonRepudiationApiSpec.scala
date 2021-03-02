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
  testing,
}
import com.daml.ports.FreePort
import com.google.common.io.BaseEncoding
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, OptionValues}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

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

  it should "correctly retrieve saved payloads by identifier" in withApi {
    (baseUrl, _, signedPayloads) =>
      val commandId = UUID.randomUUID.toString

      val firstSignedPayload =
        SignedPayload(
          algorithm = AlgorithmString.wrap("SuperCryptoStuff256"),
          fingerprint = FingerprintBytes.wrap("fingerprint-1".getBytes),
          payload = PayloadBytes.wrap(testing.generateCommand(commandId = commandId).toByteArray),
          signature = SignatureBytes.wrap("signature-1".getBytes),
          timestamp = Instant.ofEpochMilli(42),
        )

      val firstExpectedResponse = SignedPayloadsEndpoint.toResponse(firstSignedPayload)

      val secondSignedPayload =
        SignedPayload(
          algorithm = AlgorithmString.wrap("SuperCryptoStuff512"),
          fingerprint = FingerprintBytes.wrap("fingerprint-2".getBytes),
          payload = PayloadBytes.wrap(testing.generateCommand(commandId = commandId).toByteArray),
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

  it should "correctly show a certificate in the backend" in withApi { (baseUrl, certificates, _) =>
    val (_, expectedCertificate) = testing.generateKeyAndCertificate()
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

  it should "correctly upload a certificate to the backend" in withApi {
    (baseUrl, certificates, _) =>
      val (_, expectedCertificate) = testing.generateKeyAndCertificate()
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

  def withApi(
      test: (
          String,
          CertificateRepository,
          SignedPayloadRepository[CommandIdString],
      ) => Future[Assertion]
  ): Future[Assertion] = {

    val certificates = new testing.Certificates
    val signedPayloads = new testing.SignedPayloads[CommandIdString]

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

  def getSignedPayload(baseUrl: String, commandId: String): Future[HttpResponse] =
    Http().singleRequest(Get(s"$baseUrl/v1/command/$commandId"))

  def getCertificate(baseUrl: String, fingerprint: String): Future[HttpResponse] =
    Http().singleRequest(Get(s"$baseUrl/v1/certificate/$fingerprint"))

  def putCertificate(baseUrl: String, certificate: X509Certificate): Future[HttpResponse] = {
    val encodedCertificate = BaseEncoding.base64Url().encode(certificate.getEncoded)
    val request = CertificatesEndpoint.Certificate(encodedCertificate)
    val json = certificateFormat.write(request).toString()
    val entity = HttpEntity(contentType = ContentTypes.`application/json`, json)
    Http().singleRequest(Put(s"$baseUrl/v1/certificate").withEntity(entity))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    val _ = Await.result(actorSystem.terminate(), 5.seconds)
  }

}

object NonRepudiationApiSpec {

  def toSignedPayload(response: SignedPayloadsEndpoint.Response): SignedPayload =
    SignedPayload(
      algorithm = AlgorithmString.wrap(response.algorithm),
      fingerprint = FingerprintBytes.wrap(BaseEncoding.base64Url().decode(response.fingerprint)),
      payload = PayloadBytes.wrap(BaseEncoding.base64Url().decode(response.payload)),
      signature = SignatureBytes.wrap(BaseEncoding.base64Url().decode(response.signature)),
      timestamp = Instant.ofEpochMilli(response.timestamp),
    )

}
