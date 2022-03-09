// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import com.daml.scalautil.Statement.discard

import javax.crypto.Mac

/*
 * Use Mac prototypes as a workaround for
 * https://bugs.openjdk.java.net/browse/JDK-7092821, similar to Guava's
 * workaround https://github.com/google/guava/issues/1197
 */
final class MacPrototype(val algorithm: String) {
  private val prototype = createMac

  private val supportsClone: Boolean =
    try {
      discard(prototype.clone())
      true
    } catch {
      case _: CloneNotSupportedException =>
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
  val HmacSHA_256 = new MacPrototype("HmacSHA256")
}
