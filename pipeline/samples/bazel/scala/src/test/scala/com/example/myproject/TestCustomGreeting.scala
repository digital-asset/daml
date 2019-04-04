// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.example.myproject

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import org.scalatest._

class TestCustomGreeting extends FlatSpec with Matchers {

  "A Greeter" should "say Bye world" in {
    val out = new ByteArrayOutputStream()
    new Greeter(Array(), new PrintStream(out)).hello()
    new String(out.toByteArray, StandardCharsets.UTF_8).trim should be("Bye world")
  }

  it should "say Bye <args.head>" in {
    val out = new ByteArrayOutputStream()
    new Greeter(Array("toto"), new PrintStream(out)).hello()
    new String(out.toByteArray, StandardCharsets.UTF_8).trim should be("Bye toto")
  }

}
