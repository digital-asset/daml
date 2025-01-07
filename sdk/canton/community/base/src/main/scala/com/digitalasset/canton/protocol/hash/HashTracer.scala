// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.hash

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.protocol.hash.HashTracer.StringHashTracer.*
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters.*

/** Interface to provide observability in the hashing algorithm.
  * Use for debugging.
  */
sealed trait HashTracer {
  def traceByteString(bytes: ByteString, context: => String): Unit
  def traceByteArray(bytes: Array[Byte], context: => String): Unit
  def traceByte(byte: Byte, context: => String): Unit
  def context(context: String): Unit
  def subNodeTracer: HashTracer
}

object HashTracer {
  private val lineSeparator = System.lineSeparator()
  case object NoOp extends HashTracer {
    override def traceByteString(bytes: ByteString, context: => String): Unit = {}
    override def traceByteArray(bytes: Array[Byte], context: => String): Unit = {}
    override def traceByte(byte: Byte, context: => String): Unit = {}
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
  final case class StringHashTracer private (
      traceSubNodes: Boolean,
      private val sb: StringBuilder,
      level: Int,
  ) extends HashTracer {
    def result: String = sb.result()

    private def crossPlatformIndent(string: String, indent: Int) =
      string.indent(indent).replaceAll("\n", lineSeparator)

    // We indent the encoding based on the depth, this allows to visually represented nested nodes
    private def appendLine(string: String): Unit =
      discard(sb.append(crossPlatformIndent(string, level * 2))) // indent with 2 whitespaces

    override def traceByte(b: Byte, context: => String): Unit =
      appendLine(inlineValue(Bytes.fromByteArray(Array(b)).toHexString) + inlineContext(context))

    override def traceByteArray(b: Array[Byte], context: => String) =
      appendLine(inlineValue(Bytes.fromByteArray(b).toHexString) + inlineContext(context))
    override def traceByteString(byteBuffer: ByteString, context: => String): Unit =
      traceByteArray(byteBuffer.toByteArray, context)

    /** Returns a byte array of the encoding performed through this HashTracer.
      * traceSubNodes does not affect the result of this function, only encoded bytes at the first level are returned.
      */
    def asByteArray: Array[Byte] =
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

    def context(context: String): Unit =
      appendLine(context)

    override lazy val subNodeTracer: HashTracer =
      if (traceSubNodes) this.copy(level = level + 1) else HashTracer.NoOp
  }
}
