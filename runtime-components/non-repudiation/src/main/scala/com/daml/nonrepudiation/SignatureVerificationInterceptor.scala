// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.io.ByteArrayInputStream
import java.security.{PublicKey, Signature}

import com.daml.grpc.interceptors.ForwardingServerCallListener
import io.grpc.Metadata.Key
import io.grpc._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

final class SignatureVerificationInterceptor(
    keyRepository: KeyRepository.Read,
    signedPayloads: SignedPayloadRepository.Write,
) extends ServerInterceptor {

  import SignatureVerificationInterceptor._

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      metadata: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {

    val signatureData =
      for {
        signature <- getHeader(metadata, Headers.SIGNATURE, SignatureBytes.wrap)
        algorithm <- getHeader(metadata, Headers.ALGORITHM, AlgorithmString.wrap)
        fingerprint <- getHeader(metadata, Headers.FINGERPRINT, FingerprintBytes.wrap)
        key <- getKey(keyRepository, fingerprint)
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
          signedPayloads = signedPayloads,
        )
      case Left(rejection) =>
        rejection.report()
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

    def missingKey(fingerprint: String): Rejection =
      Error(s"No key found for fingerprint $fingerprint")

    val KeyVerificationFailed: Rejection =
      Error("Key verification failed")

    final case class Error(description: String) extends Rejection

    final case class Failure(description: String, cause: Throwable) extends Rejection

  }

  private trait Rejection {
    def report(): Unit = {
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
      keys: KeyRepository.Read,
      fingerprint: FingerprintBytes,
  ): Either[Rejection, PublicKey] = {
    logger.trace("Retrieving key for fingerprint '{}'", fingerprint.base64)
    keys.get(fingerprint).toRight(Rejection.missingKey(fingerprint.base64))
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
      signedPayloads: SignedPayloadRepository.Write,
  ) extends ForwardingServerCallListener(call, metadata, next) {

    private def castToByteArray(request: ReqT): Either[Rejection, Array[Byte]] = {
      logger.trace("Casting request to byte array")
      Try(request.asInstanceOf[Array[Byte]]).toEither.left.map(Rejection.fromException)
    }

    private def verifySignature(payload: Array[Byte]): Either[Rejection, Boolean] =
      Try {
        logger.trace("Decoding signature bytes from Base64-encoded signature")
        logger.trace("Initializing signature verifier")
        val verifier = Signature.getInstance(signatureData.algorithm)
        verifier.initVerify(signatureData.key)
        verifier.update(payload)
        logger.trace("Verifying signature '{}'", signatureData.signature)
        verifier.verify(signatureData.signature)
      }.toEither.left
        .map(Rejection.fromException)
        .filterOrElse(identity, Rejection.KeyVerificationFailed)

    private def addSignedCommand(
        payload: Array[Byte]
    ): Either[Rejection, Unit] = {
      logger.trace("Adding signed payload")
      val signedPayload = signatureData.toSignedPayload(payload)
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
          val dup = call.getMethodDescriptor.parseRequest(input)
          super.onMessage(dup)
        }

      result.left.foreach { rejection =>
        rejection.report()
        call.close(SignatureVerificationFailed, new Metadata())
      }
    }

  }

}
