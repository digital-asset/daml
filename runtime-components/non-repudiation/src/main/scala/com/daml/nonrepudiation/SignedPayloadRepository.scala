// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import com.daml.metrics.api.MetricHandle.Timer
import com.daml.nonrepudiation.SignedPayloadRepository.KeyEncoder

object SignedPayloadRepository {

  object KeyEncoder {
    implicit object Base64EncodePayload extends KeyEncoder[String] {
      override def encode(payload: PayloadBytes): String =
        payload.base64
    }
    implicit object ParseCommandId extends KeyEncoder[CommandIdString] {
      override def encode(payload: PayloadBytes): CommandIdString =
        CommandIdString.assertFromPayload(payload)
    }
  }

  trait KeyEncoder[Key] {
    def encode(payload: PayloadBytes): Key
  }

  trait Read[Key] {
    def get(key: Key): Iterable[SignedPayload]
  }

  trait Write {
    def put(signedPayload: SignedPayload): Unit
  }

  final class Timed(timer: Timer, delegate: Write) extends Write {
    override def put(signedPayload: SignedPayload): Unit =
      timer.time[Unit](delegate.put(signedPayload))
  }

}

abstract class SignedPayloadRepository[Key](implicit val keyEncoder: KeyEncoder[Key])
    extends SignedPayloadRepository.Read[Key]
    with SignedPayloadRepository.Write
