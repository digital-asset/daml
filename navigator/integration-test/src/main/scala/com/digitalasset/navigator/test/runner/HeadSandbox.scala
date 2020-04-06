// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test.runner

import java.io.File

import scala.concurrent._
import scala.concurrent.duration._
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Success

/**
  * Run the HEAD version of the Sandbox from source.
  *
  * Note: This will break if the relative location of the Navigator and Sandbox source code changes
  */
object HeadSandbox {

  /**
    * A logger that forwards all output to the default system output,
    * while scanning it for signs of life from the sandbox.
    */
  class SandboxLogger extends ProcessLogger {
    // Line that is printed when Sandbox has started. This is one line from a multi-line ASCII-art "Sandbox" text.
    private def sandboxStartupString = "/ __/__ ____  ___/ / /  ___ __ __"

    private val promise: Promise[Unit] = Promise()

    // Block the current thread until
    def waitForStartup(duration: Duration): Unit = {
      Await.ready(promise.future, duration)
      ()
    }

    /** Will be called with each line read from the process output stream.
      */
    def out(s: => String): Unit = {
      System.out.println(s)
      if (s.contains(sandboxStartupString)) {
        promise.complete(Success(()))
      }
    }

    /** Will be called with each line read from the process error stream.
      */
    def err(s: => String): Unit = System.err.println(s)

    /** If a process is begun with one of these `ProcessBuilder` methods:
      *  {{{
      *    def !(log: ProcessLogger): Int
      *    def !<(log: ProcessLogger): Int
      *  }}}
      *  The run will be wrapped in a call to buffer.  This gives the logger
      *  an opportunity to set up and tear down buffering.  At present the
      *  library implementations of `ProcessLogger` simply execute the body
      *  unbuffered.
      */
    def buffer[T](f: => T): T = f
  }

  def runAsync(port: Int, darFile: File, scenario: String): Unit => Unit = {
    // Run the sandbox.
    val logger = new SandboxLogger
    val sandbox = Process(
      Seq("sbt", s"sandbox/run ${darFile.getAbsolutePath} --port $port --scenario $scenario"),
      new File("../../../ledger"))
      .run(logger)

    // Sbt takes a long time to compile and start up, longer than Navigator keeps trying to connect.
    // Block for a while until the sandbox shows signs of being started up.
    logger.waitForStartup(300.seconds)

    val shutdown = (_: Unit) => {
      sandbox.destroy()
    }

    sys addShutdownHook shutdown(())
    _ =>
      shutdown(())
  }
}
