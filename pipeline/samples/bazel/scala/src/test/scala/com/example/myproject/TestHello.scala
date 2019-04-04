// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.example.myproject

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import org.scalatest._

class TestHello extends FlatSpec with Matchers {

  "A Greeter" should "say Hello world" in {
    val out = new ByteArrayOutputStream()
    new Greeter(Array(), new PrintStream(out)).hello()
    new String(out.toByteArray, StandardCharsets.UTF_8).trim should be("Hello world")
  }

  it should "say Hello <args.head>" in {
    val out = new ByteArrayOutputStream()
    new Greeter(Array("toto"), new PrintStream(out)).hello()
    new String(out.toByteArray, StandardCharsets.UTF_8).trim should be("Hello toto")
  }

}
