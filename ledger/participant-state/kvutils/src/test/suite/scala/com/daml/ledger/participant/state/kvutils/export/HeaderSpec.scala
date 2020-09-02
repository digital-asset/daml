// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{PipedInputStream, PipedOutputStream}

import com.google.common.io.ByteStreams
import org.scalatest.{Matchers, WordSpec}

final class HeaderSpec extends WordSpec with Matchers {
  "header" should {
    "write and verify the version" in {
      val input = new PipedInputStream
      val output = new PipedOutputStream(input)
      val header = new Header(version = "v1")

      header.write(output)
      output.close()

      header.consumeAndVerify(input)
      succeed
    }

    "throw if the preamble is wrong" in {
      val input = new PipedInputStream
      val output = new PipedOutputStream(input)
      val header = new Header(version = "version two")

      output.write("daml.kvutils.something\u0000version two\u0000".getBytes(Header.charset))
      output.close()

      a[Header.InvalidExportHeaderException] should be thrownBy header.consumeAndVerify(input)
    }

    "throw if the preamble is too long" in {
      val input = new PipedInputStream
      val output = new PipedOutputStream(input)
      val header = new Header(version = "vee three")

      output.write(s"${Header.preamble}.xyz\u0000version two\u0000".getBytes(Header.charset))
      output.close()

      a[Header.InvalidExportHeaderException] should be thrownBy header.consumeAndVerify(input)
    }

    "throw if the version is wrong" in {
      val input = new PipedInputStream
      val output = new PipedOutputStream(input)
      val exportHeader = new Header(version = "wzn fuor")
      val importHeader = new Header(version = "vsn four")

      exportHeader.write(output)
      output.close()

      a[Header.InvalidExportHeaderException] should be thrownBy importHeader.consumeAndVerify(input)
    }

    "throw if the version is too long" in {
      val input = new PipedInputStream
      val output = new PipedOutputStream(input)
      val exportHeader = new Header(version = "quersion 5.5")
      val importHeader = new Header(version = "quersion 5")

      exportHeader.write(output)
      output.close()

      a[Header.InvalidExportHeaderException] should be thrownBy importHeader.consumeAndVerify(input)
    }

    "flushes correctly, and does not buffer" in {
      val input = new PipedInputStream
      val output = new PipedOutputStream(input)
      val header = new Header(version = "six is the version")
      val expectedContent = "and some more stuff"

      header.write(output)
      output.write(expectedContent.getBytes(Header.charset))
      output.close()

      header.consumeAndVerify(input)
      val actualContent = new String(ByteStreams.toByteArray(input), Header.charset)
      actualContent should be(expectedContent)
    }
  }
}
