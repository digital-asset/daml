// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.example.myproject

import java.io.{InputStream, PrintStream}
import java.util.Scanner

/**
  * Prints a greeting which can be customized by building with resources and/or passing in command-
  * line arguments.
  *
  * <p>Building and running this file will print "Hello world". Build and run the
  * //pipeline/samples/bazel/scala/src/main/scala/com/example/myproject:hello-world target to demonstrate
  * this.</p>
  *
  * <p>If this is built with a greeting.txt resource included, it will replace "Hello" with the
  * contents of greeting.txt. The
  * //pipeline/samples/bazel/scala/src/main/scala/com/example/myproject:hello-resources target demonstrates
  * this.</p>
  *
  * <p>If arguments are passed to the binary on the command line, the first argument will replace
  * "world" in the output. See //pipeline/samples/bazel/scala/src/test/scala/com/example/myproject:hello's
  * argument test.</p>
  */
object Greeter extends App {
  def convertStreamToString(is: InputStream): String = scala.io.Source.fromInputStream(is).mkString

  new Greeter(args, System.out).hello()
}

class Greeter(args: Array[String], out: PrintStream) {

  def hello(): Unit = {
    hello(if (args.length > 0) args(0) else "world")
  }

  def hello(who: String): Unit = {
    val stream = classOf[Greeter].getResourceAsStream("/greeting.txt")
    var greeting = if (stream != null) Greeter.convertStreamToString(stream).trim else "Hello"
    out.println(greeting + " " + who)
  }
}
