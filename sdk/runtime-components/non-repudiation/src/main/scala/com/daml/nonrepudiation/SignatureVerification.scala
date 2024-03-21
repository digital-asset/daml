// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import java.security.Signature

import com.codahale.metrics.Timer
import org.slf4j.LoggerFactory

import scala.util.Try

object SignatureVerification {

  private val logger = LoggerFactory.getLogger(classOf[SignatureVerification])

  final class Timed(timer: Timer) extends SignatureVerification {
    override def apply(payload: Array[Byte], signatureData: SignatureData): Try[Boolean] =
      timer.time(() => super.apply(payload, signatureData))
  }

}

sealed abstract class SignatureVerification {

  import SignatureVerification.logger

  def apply(payload: Array[Byte], signatureData: SignatureData): Try[Boolean] =
    Try {
      logger.trace("Decoding signature bytes from Base64-encoded signature")
      logger.trace("Initializing signature verifier")
      val verifier = Signature.getInstance(signatureData.algorithm)
      verifier.initVerify(signatureData.key)
      verifier.update(payload)
      logger.trace("Verifying signature '{}'", signatureData.signature.base64)
      verifier.verify(signatureData.signature.unsafeArray)
    }

}
