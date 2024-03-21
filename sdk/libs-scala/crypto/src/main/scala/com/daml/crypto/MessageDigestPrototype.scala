// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import com.daml.scalautil.Statement.discard
import org.slf4j.LoggerFactory

import java.security.MessageDigest

/*
 * Use MessageDigest prototypes as a workaround for
 * https://bugs.openjdk.java.net/browse/JDK-7092821, similar to Guava's
 * workaround https://github.com/google/guava/issues/1197
 */
final class MessageDigestPrototype(val algorithm: String) {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private def createDigest: MessageDigest = MessageDigest.getInstance(algorithm)

  private val prototype = createDigest

  private val supportsClone: Boolean =
    try {
      discard(prototype.clone())
      true
    } catch {
      case _: CloneNotSupportedException =>
        logger.warn(
          s"${prototype.getClass.getName}.clone() is not supported. It might have implications on performance."
        )
        false
    }

  def newDigest: MessageDigest = {
    if (supportsClone)
      prototype.clone().asInstanceOf[MessageDigest]
    else
      createDigest
  }
}

object MessageDigestPrototype {
  final val Sha256 = new MessageDigestPrototype("SHA-256")
}
