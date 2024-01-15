// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.io.ByteArrayInputStream
import java.security.PublicKey
import java.time.Clock

import com.daml.grpc.interceptors.ForwardingServerCallListener
import com.daml.metrics.api.MetricHandle.{Meter, Timer}
import io.grpc.Metadata.Key
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, Status}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

final class SignatureVerificationInterceptor(
    certificateRepository: CertificateRepository.Read,
    signedPayloadRepository: SignedPayloadRepository.Write,
    timestampProvider: Clock,
    metrics: Metrics,
) extends ServerInterceptor {

  import SignatureVerificationInterceptor._

  private val timedCertificateRepository =
    new CertificateRepository.Timed(metrics.getKeyTimer, certificateRepository)

  private val timedSignedPayloadRepository =
    new SignedPayloadRepository.Timed(metrics.addSignedPayloadTimer, signedPayloadRepository)

  private val timedSignatureVerification =
    new SignatureVerification.Timed(metrics.verifySignatureTimer)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      metadata: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val runningTimer = metrics.processingTimer.startAsync()

    val signatureData =
      for {
        signature <- getHeader(metadata, Headers.SIGNATURE, SignatureBytes.wrap)
        algorithm <- getHeader(metadata, Headers.ALGORITHM, AlgorithmString.wrap)
        fingerprint <- getHeader(metadata, Headers.FINGERPRINT, FingerprintBytes.wrap)
        key <- getKey(timedCertificateRepository, fingerprint)
      } yield SignatureData(
        signature = signature,
        algorithm = algorithm,
        fingerprint = fingerprint,
        key = key,
      )

    signatureData match {
      case Right(signatureData) =>
        new SignatureVerificationServerCallListener(
          call = call,
          metadata = metadata,
          next = next,
          signatureData = signatureData,
          signatureVerification = timedSignatureVerification,
          signedPayloads = timedSignedPayloadRepository,
          timestampProvider = timestampProvider,
          runningTimer = runningTimer,
          rejectionMeter = metrics.rejectionsMeter,
        )
      case Left(rejection) =>
        rejection.report(metrics.rejectionsMeter, runningTimer)
        call.close(SignatureVerificationFailed, new Metadata())
        new ServerCall.Listener[ReqT] {}
    }

  }

}

object SignatureVerificationInterceptor {

  private object Rejection {

    def fromException(throwable: Throwable): Rejection =
      Failure("An exception was thrown while verifying the signature", throwable)

    def missingHeader[A](header: Key[A]): Rejection =
      Error(s"Malformed request did not contain header '${header.name}'")

    def missingCertificate(fingerprint: String): Rejection =
      Error(s"No certificate found for fingerprint $fingerprint")

    val SignatureVerificationFailed: Rejection =
      Error("Signature verification failed")

    final case class Error(description: String) extends Rejection

    final case class Failure(description: String, cause: Throwable) extends Rejection

  }

  private trait Rejection {
    def report(rejectionMeter: Meter, timer: Timer.TimerHandle): Unit = {
      rejectionMeter.mark()
      timer.stop()
      this match {
        case Rejection.Error(reason) =>
          logger.debug(reason)
        case Rejection.Failure(reason, cause) =>
          logger.debug(reason, cause)
      }
    }
  }

  val SignatureVerificationFailed: Status =
    Status.UNAUTHENTICATED.withDescription("Signature verification failed")

  private def getKey(
      certificates: CertificateRepository.Read,
      fingerprint: FingerprintBytes,
  ): Either[Rejection, PublicKey] = {
    logger.trace("Retrieving key for fingerprint '{}'", fingerprint.base64)
    certificates
      .get(fingerprint)
      .toRight(Rejection.missingCertificate(fingerprint.base64))
      .map(_.getPublicKey)
  }

  private def getHeader[Raw, Wrapped](
      metadata: Metadata,
      key: Key[Raw],
      wrap: Raw => Wrapped,
  ): Either[Rejection, Wrapped] = {
    logger.trace("Reading header '{}' from request", key.name)
    Option(metadata.get(key)).toRight(Rejection.missingHeader(key)).map(wrap)
  }

  private val logger: Logger = LoggerFactory.getLogger(classOf[SignatureVerificationInterceptor])

  private final class SignatureVerificationServerCallListener[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      metadata: Metadata,
      next: ServerCallHandler[ReqT, RespT],
      signatureData: SignatureData,
      signatureVerification: SignatureVerification,
      signedPayloads: SignedPayloadRepository.Write,
      timestampProvider: Clock,
      runningTimer: Timer.TimerHandle,
      rejectionMeter: Meter,
  ) extends ForwardingServerCallListener(call, metadata, next) {

    private def castToByteArray(request: ReqT): Either[Rejection, Array[Byte]] = {
      logger.trace("Casting request to byte array")
      Try(request.asInstanceOf[Array[Byte]]).toEither.left.map(Rejection.fromException)
    }

    private def verifySignature(payload: Array[Byte]): Either[Rejection, Boolean] =
      signatureVerification(payload, signatureData).toEither.left
        .map(Rejection.fromException)
        .filterOrElse(identity, Rejection.SignatureVerificationFailed)

    private def addSignedCommand(
        payload: Array[Byte]
    ): Either[Rejection, Unit] = {
      logger.trace("Adding signed payload")
      val signedPayload = SignedPayload(
        algorithm = signatureData.algorithm,
        fingerprint = signatureData.fingerprint,
        payload = PayloadBytes.wrap(payload),
        signature = signatureData.signature,
        timestamp = timestampProvider.instant(),
      )
      Try(signedPayloads.put(signedPayload)).toEither.left.map(Rejection.fromException)
    }

    override def onMessage(request: ReqT): Unit = {
      val result =
        for {
          payload <- castToByteArray(request)
          _ <- verifySignature(payload)
          _ <- addSignedCommand(payload)
        } yield {
          val input = new ByteArrayInputStream(payload)
          val copy = call.getMethodDescriptor.parseRequest(input)
          runningTimer.stop()
          super.onMessage(copy)
        }

      result.left.foreach { rejection =>
        rejection.report(rejectionMeter, runningTimer)
        call.close(SignatureVerificationFailed, new Metadata())
      }
    }

  }

}
