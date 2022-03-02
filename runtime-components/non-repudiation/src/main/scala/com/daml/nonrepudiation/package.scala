// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.security.PrivateKey
import java.security.cert.X509Certificate

import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.google.common.io.BaseEncoding

import scala.collection.immutable.ArraySeq
import scala.util.Try

package object nonrepudiation {

  type AlgorithmString <: String

  object AlgorithmString {
    def wrap(string: String): AlgorithmString = string.asInstanceOf[AlgorithmString]

    val RSA: AlgorithmString = wrap("RSA")
    val SHA256withRSA: AlgorithmString = wrap("SHA256withRSA")
  }

  type CommandIdString <: String

  object CommandIdString {
    def wrap(string: String): CommandIdString = string.asInstanceOf[CommandIdString]

    private val BadInput = "The mandatory `commands` field is absent"

    private def throwBadInput: Nothing = throw new IllegalArgumentException(BadInput)

    private def getCommandsOrThrow(maybeCommands: Option[Commands]): Commands =
      maybeCommands.getOrElse(throwBadInput)

    private def parseFromSubmit(payload: Array[Byte]): Try[Option[Commands]] =
      Try(SubmitRequest.parseFrom(payload).commands)

    private def parseFromSubmitAndWait(payload: Array[Byte]): Try[Option[Commands]] =
      Try(SubmitAndWaitRequest.parseFrom(payload).commands)

    private def commands(payload: Array[Byte]): Try[Commands] =
      parseFromSubmit(payload).orElse(parseFromSubmitAndWait(payload)).map(getCommandsOrThrow)

    def fromPayload(payload: PayloadBytes): Try[CommandIdString] =
      commands(payload.unsafeArray).map(_.commandId).map(wrap)

    def assertFromPayload(payload: PayloadBytes): CommandIdString =
      fromPayload(payload).get
  }

  type FingerprintBytes <: ArraySeq.ofByte

  object FingerprintBytes {
    def wrap(bytes: Array[Byte]): FingerprintBytes =
      ArraySeq.unsafeWrapArray(bytes).asInstanceOf[FingerprintBytes]
    def compute(certificate: X509Certificate): FingerprintBytes =
      wrap(Fingerprints.compute(certificate))
  }

  type PayloadBytes <: ArraySeq.ofByte

  object PayloadBytes {
    def wrap(bytes: Array[Byte]): PayloadBytes =
      new ArraySeq.ofByte(bytes).asInstanceOf[PayloadBytes]
  }

  type SignatureBytes <: ArraySeq.ofByte

  object SignatureBytes {
    def wrap(bytes: Array[Byte]): SignatureBytes =
      new ArraySeq.ofByte(bytes).asInstanceOf[SignatureBytes]
    def sign(algorithm: AlgorithmString, key: PrivateKey, payload: PayloadBytes): SignatureBytes =
      wrap(Signatures.sign(algorithm, key, payload.unsafeArray))
  }

  implicit final class BytesToBase64[Bytes <: ArraySeq.ofByte](val bytes: Bytes) extends AnyVal {
    def base64: String = BaseEncoding.base64().encode(bytes.unsafeArray)
  }

}
