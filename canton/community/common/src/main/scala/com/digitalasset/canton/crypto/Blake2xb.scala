// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.DiscardOps
import org.bouncycastle.crypto.digests.canton.Blake2bDigest

/** Derives Blake2xb on top of Blake2b as defined in:
  *  https://www.blake2.net/blake2x.pdf
  * In particular, the parameters for Blake2b invocations are as specified in that document
  */
object Blake2xb {
  private val MAX_2B_BYTES = 64

  private def b2(xofLength: Int, offset: Int, digestLength: Int, h0: Array[Byte]): Array[Byte] = {
    val nodeOffset: Long = (xofLength.toLong << 32) + offset.toLong
    Blake2bDigest.digest(h0, digestLength, 0, 0, MAX_2B_BYTES, nodeOffset, 0, MAX_2B_BYTES)
  }

  def digest(bytes: Array[Byte], outputBytes: Int): Array[Byte] = {
    val h0offset: Long = outputBytes.toLong << 32
    val h0 = Blake2bDigest.digest(bytes, MAX_2B_BYTES, 1, 1, 0, h0offset, 0, 0)
    val out = new Array[Byte](outputBytes)
    val nrChunks = outputBytes / MAX_2B_BYTES
    val lastChunkLen = outputBytes % MAX_2B_BYTES
    for (i <- 0.until(nrChunks)) {
      b2(outputBytes, i, MAX_2B_BYTES, h0).copyToArray(out, i * MAX_2B_BYTES).discard[Int]
    }
    b2(outputBytes, nrChunks, lastChunkLen, h0).copyToArray(out, nrChunks * MAX_2B_BYTES).discard
    out
  }
}
