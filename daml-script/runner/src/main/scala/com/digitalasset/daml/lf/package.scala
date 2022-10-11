// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

package object lf {

  def nowString(): String = {
    import java.util.Calendar
    import java.text.SimpleDateFormat
    val now = Calendar.getInstance().getTime()
    val logFormat = new SimpleDateFormat("hh:mm:ss.SSS")
    logFormat.format(now)
  }

  def mylog(s: String): Unit = { // NICK
    import java.io.PrintWriter
    import java.io.FileWriter
    new PrintWriter(new FileWriter("/tmp/nick.log", true)) {
      write(s"[${nowString()}] ");
      write(s);
      write("\n");
      close
    }
    // System.err.println(s"**mylog(sderr):$s")
    // println(s"**mylog:$s")
    ()
  }

  import scala.concurrent.{ExecutionContext, Future}

  implicit val ec: ExecutionContext = ExecutionContext.global

  def mylogf[T](mes: String, fut: Future[T]): Future[T] = {
    fut.map { x =>
      mylog(s"$mes$x") // show fut value
      x
    }
  }

  def mylogfx[T](mes: String, fut: Future[T]): Future[T] = {
    fut.map { x =>
      mylog(mes) // dont show fut value
      x
    }
  }

}
