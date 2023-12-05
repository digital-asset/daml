// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.demo

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.console.ConsoleEnvironment
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging
import com.digitalasset.canton.logging.TracedLogger
import org.slf4j.LoggerFactory

import java.net.URL
import java.security.MessageDigest
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Notify {

  import java.net.NetworkInterface

  import scala.jdk.CollectionConverters.*

  def send(): Unit = {

    val hasher = MessageDigest.getInstance("SHA-256")
    // create an anonymous unique user id where nobody can reconstruct who it was.
    val uid = NetworkInterface.getNetworkInterfaces.asScala
      .flatMap(x => Option(x.getHardwareAddress).toList) // get hardware addresses, filter out nulls
      .map(x => hasher.digest(x)) // hash them so people are anonymized
      .map(x => java.util.Base64.getUrlEncoder.encode(x)) // encode as base64
      .map(x => new String(x)) // map to string
      .toSeq
      .sorted // sort so that we get the same user
      .headOption
      .getOrElse("unknown")

    val url = new URL(
      s"""http://www.google-analytics.com/collect?v=1&t=event&cid=$uid&tid=UA-64532708-4&ec=demo&ea=feedback&el=start"""
        .stripMargin('|')
    )
    // try to post the notification, but don't really care if we fail
    Try(url.openStream()) match {
      case Success(is) =>
        try {
          @tailrec
          def readStream(): Unit = {
            if (is.read() != -1)
              readStream()
          }
          readStream()
        } finally {
          is.close();
        }
      case Failure(_) =>
    }

  }
}

class DemoRunner(ui: DemoUI)(implicit env: ConsoleEnvironment) extends FlagCloseable {

  override protected def logger: TracedLogger =
    logging.TracedLogger(LoggerFactory.getLogger(classOf[DemoRunner]))
  override protected val timeouts: ProcessingTimeout = ProcessingTimeout()

  env.environment.addUserCloseable(this)

  def runUI(): Unit = {
    ui.main(Array())
    if (!this.isClosing) {
      close()
      env.close()
      // we can not shutdown ammonite from another thread, so the only way
      // here is to just exit the VM
      sys.exit(0)
    }
  }

  def startBackground(): Unit = {
    val thread = new Thread(() => runUI())
    thread.setName("demo-runner")
    thread.setDaemon(true)
    thread.start()
  }

  override def onClosed(): Unit = {
    ui.backgroundCloseStage()
    ui.close()
  }
}
