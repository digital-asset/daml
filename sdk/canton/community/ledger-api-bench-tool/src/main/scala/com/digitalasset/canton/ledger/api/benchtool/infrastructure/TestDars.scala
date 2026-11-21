// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.infrastructure

import com.digitalasset.canton.ledger.api.benchtool.util.SimpleFileReader
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.archive.{DamlLf, Dar, DarParser}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString

import scala.util.chaining.*
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object TestDars {
  private val BenchtoolTestsDar = "benchtool-tests.dar"
  private lazy val resources: List[String] = List(BenchtoolTestsDar)

  val benchtoolDar: Dar[DamlLf.Archive] =
    JarResourceUtils
      .extractFileFromJar(BenchtoolTestsDar)
      .pipe(DarParser.assertReadArchiveFromFile)

  val benchtoolDarPackageRef: Ref.PackageRef =
    Ref.PackageRef.Name(Ref.PackageName.assertFromString("benchtool-tests"))

  def readAll(): Try[List[DarFile]] =
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

  final case class TestDarsError(message: String, cause: Throwable)
      extends Exception(message, cause)

  final case class DarFile(name: String, bytes: ByteString)
}
