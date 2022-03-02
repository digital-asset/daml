// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.nio.file.Files
import java.util.stream.Collectors

import com.daml.ledger.api.tls.SecretsUrlTest._
import com.daml.testing.SimpleHttpServer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Using

class SecretsUrlTest extends AnyWordSpec with Matchers {
  "a secrets URL based on a file" should {
    "open a stream" in {
      val contents = "Here is some text."
      val filePath = Files.createTempFile(getClass.getSimpleName, ".txt")
      try {
        Files.write(filePath, contents.getBytes)

        val secretsUrl = SecretsUrl.fromPath(filePath)
        val actualContents = readStreamFully(secretsUrl.openStream())

        actualContents should be(contents)
      } finally {
        Files.delete(filePath)
      }
    }
  }

  "a secrets URL based on a URL" should {
    "open a stream" in {
      val contents = "Here is a response body."
      val server = SimpleHttpServer.start(contents)
      try {
        val url = new URL(SimpleHttpServer.responseUrl(server))
        url.getProtocol should be("http")

        val secretsUrl = SecretsUrl.fromUrl(url)
        val actualContents = readStreamFully(secretsUrl.openStream())

        actualContents should be(contents)
      } finally {
        SimpleHttpServer.stop(server)
      }
    }
  }
}

object SecretsUrlTest {
  private def readStreamFully(newStream: => InputStream): String =
    Using.resource(newStream) { stream =>
      new BufferedReader(new InputStreamReader(stream))
        .lines()
        .collect(Collectors.joining(System.lineSeparator()))
    }
}
