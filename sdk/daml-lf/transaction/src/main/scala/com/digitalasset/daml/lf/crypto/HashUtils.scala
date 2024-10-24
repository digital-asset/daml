// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.Hash.underlyingHashLength
import com.digitalasset.daml.lf.data.Bytes

import java.io.OutputStream
import java.nio.ByteBuffer
import java.security.MessageDigest

object HashUtils {

  /** Extension of OutputStream with additional methods to help exporting the encoding with context information
    */
  abstract class ContextAwareOutputStream extends OutputStream {
    def write(byte: Byte): Unit
    def writeContext(context: String): Unit
  }

  /** Implementation of ContextAwareOutputStream for debugging.
    * Support contextual info and writes the encoding to a String
    */
  class DebugStringOutputStream extends ContextAwareOutputStream {
    private val sb = new StringBuilder()
    def result: String = sb.result()
    override def write(b: Int): Unit =
      discard(sb.append(Bytes.fromByteArray(Array(b.toByte)).toHexString))
    override def write(b: Array[Byte], off: Int, len: Int) = {
      discard(sb.append(Bytes.fromByteArray(b.slice(off, off + len)).toHexString))
    }
    override def write(byte: Byte): Unit =
      discard(sb.append(byte.toString))

    def writeContext(context: String): Unit = discard(sb.append(context))
  }

  /** Interface for message digest. Allows to swap out default implementation for debug implementation.
    */
  private[crypto] trait MessageDigestWithContext {
    def messageDigest: MessageDigest
    def update(a: Byte, context: Option[String]): Unit
    def update(a: ByteBuffer, context: Option[String]): Unit
    def update(a: Array[Byte], context: Option[String]): Unit
    def digest(buf: Array[Byte], offset: Int, len: Int): Int
    def addContext(context: String): Unit
    def isContextAware: Boolean
  }

  /** Default Message Digest implementation. Ignores context and simply feeds all bytes to the delegate MessageDigest.
    * @param messageDigest MessageDigest delegate which calculates the hash.
    */
  private[crypto] class DefaultMessageDigest(override val messageDigest: MessageDigest)
      extends MessageDigestWithContext {
    override val isContextAware: Boolean = false
    override def update(a: Byte, context: Option[String]): Unit = messageDigest.update(a)
    override def update(a: ByteBuffer, context: Option[String]): Unit = messageDigest.update(a)
    override def update(a: Array[Byte], context: Option[String]): Unit = messageDigest.update(a)
    override def digest(buf: Array[Byte], offset: Int, len: Int): Int =
      messageDigest.digest(buf, 0, underlyingHashLength)
    // Default message digest doesn't do anything with the context
    override def addContext(context: String): Unit = {}
  }

  /** Message Digest for debugging with support for contextual information and output stream to observe the encoding result.
    *
    * @param messageDigest MessageDigest delegate which calculates the hash.
    * @param outputStream Output stream that also receives the encoded objects along with context information.
    *
    * The format of the produced debug output is the following:
    *
    * _Context About Following Lines_
    * "encoded value in hex" - ["original value before encoding" ("additional contextual info", for instance type of the value)]
    *
    * e.g:
    *   _Package Name_ # encoding of a package name value
    *   0000000e - [14 (int)] # strings are encoded by first emitting the size of the encoded string as an int
    *   7061636b6167652d6e616d652d30 - [package-name-0 (string)] # UTF-8 encoded value of 'package-name-0'. Note that its length is indeed 14 bytes (1 byte is encoded with 2 hexadecimal values)
    */
  private[crypto] class DebugMessageDigest(
      messageDigest: MessageDigest,
      outputStream: ContextAwareOutputStream,
  ) extends DefaultMessageDigest(messageDigest) {
    override val isContextAware: Boolean = true

    override def update(a: ByteBuffer, context: Option[String]): Unit = {
      if (a.hasArray) {
        update(a.array(), context)
      } else {
        // Copied from MessageDigestSpi.engineUpdate(ByteBuffer input)
        var len = a.remaining
        val tempArray = new Array[Byte](len)
        while (len > 0) {
          val chunk = Math.min(len, tempArray.length)
          discard(a.get(tempArray, 0, chunk))
          len -= chunk
        }
        update(tempArray, context)
      }
    }

    private def contextForBytes(context: Option[String]) = {
      context
        .map(c => s" - [$c]\n")
    }

    override def update(a: Byte, context: Option[String]): Unit = {
      outputStream.write(a)
      contextForBytes(context).foreach(outputStream.writeContext)
      super.update(a, context)
    }
    override def update(a: Array[Byte], context: Option[String]): Unit = {
      outputStream.write(a)
      contextForBytes(context).foreach(outputStream.writeContext)
      super.update(a, context)
    }

    override def digest(buf: Array[Byte], offset: Int, len: Int): Int = {
      outputStream.flush()
      outputStream.close()
      super.digest(buf, offset, len)
    }

    override def addContext(context: String): Unit = outputStream.writeContext(context)
  }

}

