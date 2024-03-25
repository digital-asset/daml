// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.integrationtest.CantonFixture
import com.daml.lf.archive.DarParser
import com.daml.ports.Port
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Files}
import org.scalatest.{Suite, Succeeded}
import org.scalatest.compatible.Assertion
import scala.concurrent.{ExecutionContext, Future}

trait RunnerMainTestBaseCanton extends CantonFixture with RunnerMainTestBase {
  self: Suite =>

  private def didUpload(dar: Path): Future[Boolean] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    for {
      client <- defaultLedgerClient()
      res <- client.v2.packageService.listPackages()
      lf = DarParser.assertReadArchiveFromFile(dar.toFile)
    } yield res.packageIds.contains(lf.main.getHash)
  }

  private def assertUpload(dar: Path, shouldHaveUploaded: Boolean): Future[Assertion] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    didUpload(dar).map { result =>
      (result, shouldHaveUploaded) match {
        case (true, false) => fail("DAR was uploaded when it should not have been.")
        case (false, true) => fail("DAR was not uploaded when it should have been.")
        case _ => succeed
      }
    }
  }

  def testDamlScriptCanton(
      dar: Path,
      args: Seq[String],
      expectedResult: Either[Seq[String], Seq[String]] = Right(Seq()),
      shouldHaveUploaded: Option[Boolean] = None,
  ): Future[Assertion] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    testDamlScript(dar, args, expectedResult).flatMap {
      case Succeeded => shouldHaveUploaded.fold(Future.successful(succeed))(assertUpload(dar, _))
      case res => Future.successful(res)
    }
  }

  def withGrpcParticipantConfig[A](f: Path => Future[A]): Future[A] =
    withParticipantConfig(ports.head, f)

  def withParticipantConfig[A](port: Port, f: Path => Future[A]): Future[A] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val path = Files.createTempFile("participantConfig", ".json")
    val content =
      s"""{
         |  "default_participant": {"host": "localhost", "port": ${port.toString}},
         |  "participants": {},
         |  "party_participants": {}
         |}
         """.stripMargin
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    f(path).transform(res => {
      Files.delete(path)
      res
    })
  }
}
