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
import scala.jdk.CollectionConverters._

object HashUtils {

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

      def apply(traceSubNodes: Boolean = false) =
        new StringHashTracer(traceSubNodes, new StringBuilder(), 0)
    }

    /** Hash tracer that accumulated encoded values into a string.
      * @param traceSubNodes whether sub nodes should be traced as well (defaults to false)
      */
    case class StringHashTracer private (
        traceSubNodes: Boolean,
        private val sb: StringBuilder,
        level: Int,
    ) extends HashTracer {
      def result: String = sb.result()

      // We indent the encoding based on the depth, this allows to visually represented nested nodes
      private def appendLine(string: String): Unit = {
        discard(sb.append(string.indent(level * 2))) // indent with 2 whitespaces
      }

      override def trace(b: Byte, context: String): Unit = {
        appendLine(inlineValue(Bytes.fromByteArray(Array(b)).toHexString) + inlineContext(context))
      }

      override def trace(b: Array[Byte], context: String) = {
        appendLine(inlineValue(Bytes.fromByteArray(b).toHexString) + inlineContext(context))
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

      /** Returns a byte array of the encoding performed through this HashTracer.
        * traceSubNodes does not affect the result of this function, only encoded bytes at the first level are returned.
        */
      def asByteArray: Array[Byte] = {
        result
          .lines()
          .iterator()
          .asScala
          // The byte array only returns top level encoding, so filter out lines that are indented
          .filterNot(_.startsWith(" "))
          .flatMap(encodingRegex.findFirstIn(_))
          // Strip the quotes
          .map(_.stripPrefix(encodedValueWrapper).stripSuffix(encodedValueWrapper))
          .map(Bytes.assertFromString)
          .toArray
          .flatMap(_.toByteArray)
      }

      def context(context: String): Unit = {
        appendLine(context)
      }

      override lazy val subNodeTracer: HashTracer =
        if (traceSubNodes) this.copy(level = level + 1) else HashTracer.NoOp
    }
  }

}
