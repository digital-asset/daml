// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.example

import java.io.File
import java.net.ServerSocket

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object Util {
  def findOpenPort(): Try[Int] = Try {
    val socket = new ServerSocket(0)
    val result = socket.getLocalPort
    socket.close()
    result
  }

  def toFile(fileName: String): Try[File] = {
    val file = new File(fileName)
    if (file.exists()) Success(file.getAbsoluteFile)
    else
      Failure(new IllegalStateException(s"File doest not exist: $fileName"))
  }

  def toFiles(fileNames: List[String]): Try[List[File]] =
    traverse(fileNames)(toFile)

  private def traverse[A, B](as: List[A])(f: A => Try[B]): Try[List[B]] =
    as.foldRight(Success(Nil): Try[List[B]]) { (a, bs) =>
      map2(f(a), bs)(_ :: _)
    }

  private def map2[A, B, C](a: Try[A], b: Try[B])(f: (A, B) => C): Try[C] =
    for {
      a1 <- a
      b1 <- b
    } yield f(a1, b1)

  def toFuture[A](o: Option[A]): Future[A] = o match {
    case None => Future.failed(new IllegalStateException(s"empty option: $o"))
    case Some(x) => Future.successful(x)
  }
}
