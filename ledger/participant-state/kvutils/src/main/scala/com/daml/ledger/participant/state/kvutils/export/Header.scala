// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import com.daml.ledger.participant.state.kvutils.export.Header._

object Header {
  // Let's keep this simple. Versions must be ASCII.
  private[export] val charset = StandardCharsets.US_ASCII

  private[export] val preamble = charset.encode("daml.kvutils.export")

  final class InvalidExportHeaderException private[Header] (version: String)
      extends RuntimeException(s"Invalid export header. Expected version: $version")

}

final class Header(version: String) {
  private val versionBytes = charset.encode(version)

  def write(output: OutputStream): Unit = {
    output.write(preamble.array())
    output.write(0)
    output.write(versionBytes.array())
    output.write(0)
    output.flush()
  }

  def consumeAndVerify(input: InputStream): Unit = {
    if (!isValid(input)) {
      throw new InvalidExportHeaderException(version)
    }
  }

  def isValid(path: Path): Boolean = {
    val input = Files.newInputStream(path)
    try {
      isValid(input)
    } finally {
      input.close()
    }
  }

  private def isValid(input: InputStream): Boolean = {
    verifyChunk(input, preamble) && verifyChunk(input, versionBytes)
  }

  private def verifyChunk(input: InputStream, value: ByteBuffer): Boolean = {
    val size = value.limit() + 1
    val buffer = ByteBuffer.allocate(size)
    val charactersRead = input.read(buffer.array())
    buffer.rewind()
    (
      charactersRead == size
      && buffer.array()(value.limit()) == 0
      && buffer.limit(value.limit()).equals(value)
    )
  }
}
