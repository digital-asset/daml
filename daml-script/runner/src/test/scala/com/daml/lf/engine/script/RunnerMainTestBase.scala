// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import com.daml.http.HttpServiceTestFixture
import com.daml.integrationtest.CantonFixtureWithResource
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ports.Port
import com.daml.scalautil.Statement.discard
import java.nio.file.{Path, Paths}
import org.scalatest.compatible.Assertion
import org.scalatest.Suite
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.sys.process._
import com.daml.lf.archive.DarParser

trait RunnerMainTestBase extends CantonFixtureWithResource[Port] {
  self: Suite =>
  protected def jsonApiPort: Port = additional
  protected val jwt: Path =
    BazelRunfiles.rlocation(Paths.get("daml-script/runner/src/test/resources/json-access.jwt"))

  // Defines the size of `dars`, should always be equal to max(number of tls tests, number of non tls tests)
  // Should always match test_dar_count in the BUILD file
  val DAR_COUNT = 8

  // We use a different DAR for each test so we can correctly assert the upload behaviour.
  val dars: Seq[Path] = (1 to DAR_COUNT).map(n =>
    BazelRunfiles.rlocation(Paths.get(s"daml-script/runner/test-script$n.dar"))
  )

  implicit val ec = ExecutionContext.global

  override protected def makeAdditionalResource(
      ports: Vector[Port]
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
              ports.head,
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

  val damlScript = BazelRunfiles.rlocation(Paths.get("daml-script/runner/daml-script-binary"))

  // Runs process with args, returns out on success, stderr on failure
  private def runProc(exe: Path, args: Seq[String]): Future[Either[String, String]] =
    Future {
      val out = new StringBuilder()
      val err = new StringBuilder()
      val cmd = exe.toString +: args
      cmd !< ProcessLogger(
        line => discard(out append line),
        line => discard(err append line),
      ) match {
        case 0 => Right(out.toString)
        case _ => Left(err.toString)
      }
    }

  private def didUpload(dar: Path): Future[Boolean] =
    for {
      client <- defaultLedgerClient()
      res <- client.packageClient.listPackages()
      lf = DarParser.assertReadArchiveFromFile(dar.toFile)
    } yield res.packageIds.exists(_ == lf.main.getHash)

  private def assertUpload(dar: Path, shouldHaveUploaded: Boolean): Future[Assertion] =
    didUpload(dar).map { result =>
      (result, shouldHaveUploaded) match {
        case (true, false) => fail("DAR was uploaded when it should not have been.")
        case (false, true) => fail("DAR was not uploaded when it should have been.")
        case _ => succeed
      }
    }

  def testDamlScript(
      dar: Path,
      args: Seq[String],
      expectedResult: Either[Seq[String], Seq[String]] = Right(Seq()),
      shouldHaveUploaded: Boolean = false,
  ): Future[Assertion] =
    runProc(damlScript, Seq("--dar", dar.toString) ++ args).flatMap { res =>
      (res, expectedResult) match {
        case (Right(actual), Right(expecteds)) =>
          if (expecteds.forall(actual contains _)) assertUpload(dar, shouldHaveUploaded)
          else
            fail(
              s"Expected daml-script stdout to contain '${expecteds.mkString("', '")}' but it did not:\n$actual"
            )

        case (Left(actual), Left(expecteds)) =>
          assertUpload(dar, shouldHaveUploaded)
          if (expecteds.forall(actual contains _)) Future.successful(succeed)
          else
            fail(
              s"Expected daml-script stderr to contain '${expecteds.mkString("', '")}' but it did not:\n$actual"
            )

        case (Right(_), Left(expecteds)) =>
          fail(s"Expected daml-script to fail with ${expecteds.mkString("', '")} but it succeeded.")

        case (Left(actual), Right(_)) =>
          fail(s"Expected daml-script to succeed but it failed with $actual")
      }
    }

}
