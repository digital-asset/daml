// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.infrastructure

import com.daml.ledger.api.benchtool.util.SimpleFileReader
import com.daml.ledger.test.TestDar
import com.google.protobuf.ByteString

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object TestDars {
  private val TestDarInfix = "model-tests"
  private lazy val resources: List[String] = TestDar.paths.filter(_.contains(TestDarInfix))

  def readAll(): Try[List[DarFile]] = {
    (TestDars.resources
      .foldLeft[Try[List[DarFile]]](Success(List.empty)) { (acc, resourceName) =>
        for {
          dars <- acc
          bytes <- SimpleFileReader.readResource(resourceName)
        } yield DarFile(resourceName, bytes) :: dars
      })
      .recoverWith { case NonFatal(ex) =>
        Failure(TestDarsError(s"Reading test dars failed. Details: ${ex.getLocalizedMessage}", ex))
      }
  }

  case class TestDarsError(message: String, cause: Throwable) extends Exception(message, cause)

  case class DarFile(name: String, bytes: ByteString)
}
