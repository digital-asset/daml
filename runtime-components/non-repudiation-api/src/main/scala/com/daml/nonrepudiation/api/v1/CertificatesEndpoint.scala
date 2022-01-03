// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api.v1

import java.io.ByteArrayInputStream
import java.security.cert.{CertificateFactory, X509Certificate}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.daml.nonrepudiation.api.Result
import com.daml.nonrepudiation.{CertificateRepository, FingerprintBytes}
import com.google.common.io.BaseEncoding
import org.slf4j.{Logger, LoggerFactory}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.util.Try

private[api] final class CertificatesEndpoint private (certificates: CertificateRepository)
    extends Result.JsonProtocol
    with CertificatesEndpoint.JsonProtocol {

  import CertificatesEndpoint._

  private def putCertificate(certificate: X509Certificate): Route =
    handleExceptions(logAndReport(logger)(UnableToAddTheCertificate)) {
      val fingerprint = certificates.put(certificate)
      complete(Result.Success(encode(fingerprint), 200))
    }

  private def getCertificate(fingerprint: FingerprintBytes): Route =
    handleExceptions(logAndReport(logger)(UnableToRetrieveTheCertificate)) {
      certificates
        .get(fingerprint)
        .fold(reject)(certificate => complete(Result.Success(encode(certificate), 200)))
    }

  private val route: Route =
    concat(
      put {
        decodeRequest {
          entity(as[CertificatesEndpoint.Certificate]) {
            decode(_).fold(rejectBadInput, putCertificate)
          }
        }
      },
      path(Segment) { fingerprint =>
        get {
          decode(fingerprint).fold(rejectBadInput, getCertificate)
        }
      },
    )

}

object CertificatesEndpoint {

  def apply(certificates: CertificateRepository): Route =
    new CertificatesEndpoint(certificates).route

  private val logger: Logger = LoggerFactory.getLogger(classOf[CertificatesEndpoint])

  private[api] val InvalidCertificateString: String =
    "Invalid upload, the 'certificate' field must contain a URL-safe base64-encoded X.509 certificate"

  private[api] val InvalidCertificateFormat: String =
    "Invalid certificate format, the certificate must be a valid X.509 certificate"

  private[api] val UnableToAddTheCertificate: String =
    "An error occurred when trying to add the certificate, please try again."

  private[api] val UnableToRetrieveTheCertificate: String =
    "An error occurred when trying to retrieve the certificate, please try again."

  private[api] val InvalidFingerprintString: String =
    "Invalid request, the fingerprint must be a URL-safe base64-encoded array of bytes representing the SHA256 of the DER representation of an X.509 certificate"

  private val certificateFactory: CertificateFactory =
    CertificateFactory.getInstance("X.509")

  private def decodeCertificate(bytes: Array[Byte]): Either[String, X509Certificate] =
    Try(
      certificateFactory
        .generateCertificate(new ByteArrayInputStream(bytes))
        .asInstanceOf[X509Certificate]
    ).toEither.left.map(_ => InvalidCertificateFormat)

  private def decodeBytes(string: String, error: String): Either[String, Array[Byte]] =
    Try(BaseEncoding.base64Url().decode(string)).toEither.left.map(_ => error)

  private def decode(request: Certificate): Either[String, X509Certificate] =
    decodeBytes(request.certificate, InvalidCertificateString).flatMap(decodeCertificate)

  private def decode(fingerprint: String): Either[String, FingerprintBytes] =
    decodeBytes(fingerprint, InvalidFingerprintString).map(FingerprintBytes.wrap)

  final case class Certificate(certificate: String)

  private def encode(bytes: FingerprintBytes): Fingerprint =
    Fingerprint(BaseEncoding.base64Url().encode(bytes.unsafeArray))

  // URL-safe encoding is used for certificate as well even though they don't require
  // it to make sure there is only one encoding used across the API (fingerprints can
  // be passed in URLs for GETs and thus are required to be URL-safe)
  private def encode(certificate: X509Certificate): Certificate =
    Certificate(BaseEncoding.base64Url().encode(certificate.getEncoded))

  final case class Fingerprint(fingerprint: String)

  private[api] trait JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {

    protected implicit val certificateFormat: RootJsonFormat[Certificate] =
      jsonFormat1(Certificate.apply)

    protected implicit val fingerprintFormat: RootJsonFormat[Fingerprint] =
      jsonFormat1(Fingerprint.apply)

  }

}
