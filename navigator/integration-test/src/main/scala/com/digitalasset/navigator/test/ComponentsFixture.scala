// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test

import java.util.concurrent.atomic.AtomicReference

import com.daml.navigator.test.config.Arguments
import com.daml.navigator.test.runner.{HeadNavigator, PackagedDamlc, PackagedSandbox}
import com.typesafe.scalalogging.LazyLogging

import scala.io.Source
import scala.util.{Failure, Success, Try}

class ComponentsFixture(
    val args: Arguments,
    val navigatorPort: Int,
    val sandboxPort: Int,
    val scenario: String
) extends LazyLogging {

  // A list of commands on how to destroy started processes
  private val killProcs: AtomicReference[List[Unit => Unit]] = new AtomicReference(List.empty)

  private val onlineUrl = s"http://localhost:$navigatorPort/api/about"

  private def get(
      url: String,
      connectTimeout: Int = 1000,
      readTimeout: Int = 1000,
      requestMethod: String = "GET"
  ): String = {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close()
    content
  }

  def startup(): Try[Unit] = {
    if (args.startComponents) {
      logger.info("Starting the sandbox and the Navigator")
      for {
        (darFile, tempFiles) <- Try(PackagedDamlc.run(args.damlPath))
        sandbox <- Try(PackagedSandbox.runAsync(sandboxPort, darFile, scenario))
        _ = killProcs.updateAndGet(s => sandbox :: s)
        navigator <- Try(
          HeadNavigator.runAsync(args.navConfPAth, args.navigatorDir, navigatorPort, sandboxPort))
        _ = killProcs.updateAndGet(s => navigator :: s)
      } yield { () }
    } else {
      Success(())
    }
  }

  private def retry[R](action: => R, maxRetries: Int, delayMillis: Int): Try[R] = {
    def retry0(count: Int): Try[R] = {
      Try(action) match {
        case Success(r) => Success(r)
        case Failure(e) =>
          if (count > maxRetries) {
            logger.error(
              s"Navigator is not available after $maxRetries retries with $delayMillis millis interval.")
            Failure(e)
          } else {
            logger.info(s"Navigator is not available yet, waiting $delayMillis millis ")
            Thread.sleep(delayMillis.toLong)
            retry0(count + 1)
          }
      }
    }

    retry0(0)
  }

  def waitForNavigator(): Try[Unit] = {
    logger.info(s"Waiting for the Navigator to start up (waiting for $onlineUrl)")
    retry({ get(onlineUrl); () }, 120, 1000)
  }

  def shutdown(): Unit = {
    killProcs.getAndUpdate(procs => {
      procs.foreach(killAction => Try { killAction(()) })
      List.empty
    })
    ()
  }
}
