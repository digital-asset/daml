// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.http.HttpServiceTestFixture
import com.daml.integrationtest.{CantonFixture, CantonFixtureWithResource}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.archive.DarParser
import com.daml.ports.Port
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Files}
import org.scalatest.{Suite, Succeeded}
import org.scalatest.compatible.Assertion
import scala.concurrent.{ExecutionContext, Future, Promise}

trait RunnerMainTestBaseCanton extends CantonFixtureWithResource[Port] with RunnerMainTestBase {
  self: Suite =>
  protected def jsonApiPort: Port = additional

  override protected def makeAdditionalResource(
      ports: Vector[CantonFixture.LedgerPorts]
  ): ResourceOwner[Port] =
    new ResourceOwner[Port] {
      override def acquire()(implicit
          context: ResourceContext
      ): Resource[Port] = {
        def start(): Future[(Port, Promise[Unit])] = {
          // Used to wait until http service is started, resolved at start of withHttpService callback, gives back port
          val startingPromise = Promise[Port]()
          // Used to stop the http service, by having withhttpService wait on it, resolved in stop()
          val stoppingPromise = Promise[Unit]()

          val useTls =
            if (tlsEnable) HttpServiceTestFixture.UseTls.Tls
            else HttpServiceTestFixture.UseTls.NoTls

          val _ =
            HttpServiceTestFixture.withHttpService(
              "NonTlsRunnerMainTest",
              ports.head.ledgerPort,
              None,
              None,
              useTls = useTls,
            ) { case (uri, _, _, _) =>
              startingPromise.success(Port(uri.effectivePort))
              stoppingPromise.future
            }

          startingPromise.future.map { port => (port, stoppingPromise) }
        }
        def stop(r: (Port, Promise[Unit])) = Future {
          r._2.success(())
          ()
        }
        Resource(start())(stop).map(_._1)
      }
    }

  private def didUpload(dar: Path): Future[Boolean] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    for {
      client <- defaultLedgerClient()
      res <- client.packageClient.listPackages()
      lf = DarParser.assertReadArchiveFromFile(dar.toFile)
    } yield res.packageIds.exists(_ == lf.main.getHash)
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
    withParticipantConfig(None, ports.head, f)

  def withJsonParticipantConfig[A](f: Path => Future[A]): Future[A] =
    withParticipantConfig(Some("http"), jsonApiPort, f)

  def withParticipantConfig[A](
      protocol: Option[String],
      port: Port,
      f: Path => Future[A],
  ): Future[A] = {
    implicit val ec: ExecutionContext = ExecutionContext.global
    val path = Files.createTempFile("participantConfig", ".json")
    val schema = protocol.fold("")(_ + "://")
    val content =
      s"""{
         |  "default_participant": {"host": "${schema}localhost", "port": ${port.toString}},
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
