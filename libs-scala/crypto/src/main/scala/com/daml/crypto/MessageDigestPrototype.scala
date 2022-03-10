// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import com.daml.scalautil.Statement.discard

import java.security.MessageDigest

/*
 * Use MessageDigest prototypes as a workaround for
 * https://bugs.openjdk.java.net/browse/JDK-7092821, similar to Guava's
 * workaround https://github.com/google/guava/issues/1197
 */
final class MessageDigestPrototype(val algorithm: String) {
  private def createDigest: MessageDigest = MessageDigest.getInstance(algorithm)

  private val prototype = createDigest

  private val supportsClone: Boolean =
    try {
      discard(prototype.clone())
      true
    } catch {
      case _: CloneNotSupportedException =>
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
  final val SHA_256 = new MessageDigestPrototype("SHA-256")
}
