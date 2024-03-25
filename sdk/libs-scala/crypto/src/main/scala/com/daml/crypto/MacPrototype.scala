// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import com.daml.scalautil.Statement.discard
import org.slf4j.LoggerFactory

import javax.crypto.Mac

/*
 * Use Mac prototypes as a workaround for
 * https://bugs.openjdk.java.net/browse/JDK-7092821, similar to Guava's
 * workaround https://github.com/google/guava/issues/1197
 */
final class MacPrototype(val algorithm: String) {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private val prototype = createMac

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

  private def createMac: Mac = Mac.getInstance(algorithm)

  def newMac: Mac = {
    if (supportsClone)
      prototype.clone().asInstanceOf[Mac]
    else
      createMac
  }
}

object MacPrototype {
  val HmacSha256 = new MacPrototype("HmacSHA256")
}
