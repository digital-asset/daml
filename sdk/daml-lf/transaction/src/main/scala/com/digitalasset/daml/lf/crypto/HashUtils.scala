// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.HashUtils.HashTracer.StringHashTracer.{
  encodedValueWrapper,
  encodingRegex,
  inlineContext,
  inlineValue,
}
import com.digitalasset.daml.lf.data.Bytes

import java.nio.ByteBuffer

object HashUtils {

  private val lineSeparator = System.getProperty("line.separator")
  private[crypto] def formatByteToHexString(byte: Byte): String = String.format("%02X", byte)

  /** Interface to provide observability in the encoding algorithm.
    * Use for debugging.
    */
  trait HashTracer {
    def trace(bytes: ByteBuffer, context: String): Unit
    def trace(bytes: Array[Byte], context: String): Unit
    def trace(byte: Byte, context: String): Unit
    def context(context: String): Unit
    def subNodeTracer: HashTracer
  }

  object HashTracer {
    case object NoOp extends HashTracer {
      override def trace(bytes: ByteBuffer, context: String): Unit = {}
      override def trace(bytes: Array[Byte], context: String): Unit = {}
      override def trace(byte: Byte, context: String): Unit = {}
      override def context(context: String): Unit = {}
      override val subNodeTracer: HashTracer = this
    }

    object StringHashTracer {
      private val encodedValueWrapper = "'"
      private val encodingRegex = s"$encodedValueWrapper[0-9a-fA-F]+$encodedValueWrapper".r
      private def inlineContext(context: String): String = s" # $context"
      private def inlineValue(value: String): String =
        s"$encodedValueWrapper$value$encodedValueWrapper"
    }

    /** Hash tracer that accumulated encoded values into a string.
      * @param traceSubNodes whether sub nodes should be traced as well (defaults to false)
      */
    class StringHashTracer(traceSubNodes: Boolean = false) extends HashTracer {
      private val sb = new StringBuilder()
      def result: String = sb.result()
      override def trace(b: Byte, context: String): Unit =
        discard(
          sb
            .append(inlineValue(Bytes.fromByteArray(Array(b)).toHexString))
            .append(inlineContext(context))
            .append(lineSeparator)
        )
      override def trace(b: Array[Byte], context: String) = {
        discard(
          sb
            .append(inlineValue(Bytes.fromByteArray(b).toHexString))
            .append(inlineContext(context))
            .append(lineSeparator)
        )
      }
      override def trace(byteBuffer: ByteBuffer, context: String): Unit = {
        if (byteBuffer.hasArray) {
          trace(byteBuffer.array(), context)
        } else {
          val array = new Array[Byte](byteBuffer.remaining())
          discard(byteBuffer.get(array))
          trace(array, context)
          discard(byteBuffer.rewind())
        }
      }

      def asByteArray: Array[Byte] = {
        encodingRegex
          .findAllMatchIn(result)
          // Strip the quotes
          .map(_.toString().stripPrefix(encodedValueWrapper).stripSuffix(encodedValueWrapper))
          .map(Bytes.assertFromString)
          .toArray
          .flatMap(_.toByteArray)
      }

      def context(context: String): Unit = discard(sb.append(context).append(lineSeparator))

      override val subNodeTracer: HashTracer = if (traceSubNodes) this else HashTracer.NoOp
    }
  }

}
