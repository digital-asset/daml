// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.security.PublicKey

import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.google.common.io.BaseEncoding

import scala.util.Try

package object nonrepudiation {

  type AlgorithmString <: String

  object AlgorithmString {
    def wrap(string: String): AlgorithmString = string.asInstanceOf[AlgorithmString]

    val SHA256withRSA: AlgorithmString = wrap("SHA256withRSA")
  }

  type CommandIdString <: String

  object CommandIdString {
    private def wrap(string: String): CommandIdString = string.asInstanceOf[CommandIdString]

    private val BadInput = "The mandatory `commands` field is absent"

    private def throwBadInput: Nothing = throw new IllegalArgumentException(BadInput)

    private def getCommandsOrThrow(maybeCommands: Option[Commands]): Commands =
      maybeCommands.getOrElse(throwBadInput)

    private def parseFromSubmit(payload: Array[Byte]): Try[Option[Commands]] =
      Try(SubmitRequest.parseFrom(payload).commands)

    private def parseFromSubmitAndWait(payload: Array[Byte]): Try[Option[Commands]] =
      Try(SubmitAndWaitRequest.parseFrom(payload).commands)

    private def commands(payload: PayloadBytes): Try[Commands] =
      parseFromSubmit(payload).orElse(parseFromSubmitAndWait(payload)).map(getCommandsOrThrow)

    def fromPayload(payload: PayloadBytes): Try[CommandIdString] =
      commands(payload).map(_.commandId).map(wrap)

    def assertFromPayload(payload: PayloadBytes): CommandIdString =
      fromPayload(payload).get
  }

  type FingerprintBytes <: Array[Byte]

  object FingerprintBytes {
    def wrap(bytes: Array[Byte]): FingerprintBytes = bytes.asInstanceOf[FingerprintBytes]
    def fromPublicKey(key: PublicKey): FingerprintBytes = wrap(Fingerprints.compute(key))
  }

  type PayloadBytes <: Array[Byte]

  object PayloadBytes {
    def wrap(bytes: Array[Byte]): PayloadBytes = bytes.asInstanceOf[PayloadBytes]
  }

  type SignatureBytes <: Array[Byte]

  object SignatureBytes {
    def wrap(bytes: Array[Byte]): SignatureBytes = bytes.asInstanceOf[SignatureBytes]
  }

  implicit final class BytesToBase64[Bytes <: Array[Byte]](val bytes: Bytes) extends AnyVal {
    def base64: String = BaseEncoding.base64().encode(bytes)
  }

}
