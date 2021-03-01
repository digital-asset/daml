// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api.v1

import java.io.ByteArrayInputStream
import java.security.cert.{CertificateFactory, X509Certificate}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, StandardRoute, ValidationRejection}
import com.daml.nonrepudiation.{CertificateRepository, FingerprintBytes}
import com.google.common.io.BaseEncoding
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.util.Try

private[api] final class CertificatesEndpoint private (certificates: CertificateRepository)
    extends CertificatesEndpoint.JsonProtocol {

  import CertificatesEndpoint._

  private def tryAdd(certificate: X509Certificate): Either[Throwable, FingerprintBytes] =
    Try(certificates.put(certificate)).toEither

  private val route: Route =
    concat(
      put {
        decodeRequest {
          entity(as[CertificatesEndpoint.Certificate]) { request =>
            decode(request)
              .fold(rejectBadInput, tryAdd(_).fold(reportServerError, fulfillRequest))
          }
        }
      },
      path(Segment) { fingerprint =>
        get {
          decode(fingerprint)
            .map(certificates.get)
            .fold(rejectBadInput, _.fold(reject)(fulfillRequest))
        }
      },
    )

}

object CertificatesEndpoint {

  def apply(certificates: CertificateRepository): Route =
    new CertificatesEndpoint(certificates).route

  private val InvalidCertificateString: String =
    "Invalid upload, the 'certificate' field must contain a URL-safe base64-encoded X.509 certificate"

  private val InvalidCertificateFormat: String =
    "Invalid certificate format, the certificate must be a valid X.509 certificate"

  private val UnableToAddTheCertificate: String =
    "An error occurred when trying to add the certificate, please try again."

  private val InvalidFingerprintString: String =
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

  private def rejectBadInput(errorMessage: String): StandardRoute =
    reject(ValidationRejection(errorMessage))

  private def reportServerError(throwable: Throwable): StandardRoute =
    failWith(new RuntimeException(UnableToAddTheCertificate, throwable))

  private def fulfillRequest(fingerprint: FingerprintBytes)(implicit
      marshaller: ToEntityMarshaller[Fingerprint]
  ): StandardRoute =
    complete(encode(fingerprint))

  private def fulfillRequest(certificate: X509Certificate)(implicit
      marshaller: ToEntityMarshaller[Certificate]
  ): StandardRoute = complete(encode(certificate))

  final case class Certificate(certificate: String)

  private def encode(bytes: FingerprintBytes): Fingerprint =
    Fingerprint(BaseEncoding.base64Url().encode(bytes.unsafeArray))

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
