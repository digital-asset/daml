// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Files

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes, Uri}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.scalautil.Statement.discard
import com.daml.http.util.TestUtil.writeToFile
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.Future

abstract class HttpServiceIntegrationTest
    extends AbstractHttpServiceIntegrationTestTokenIndependent
    with BeforeAndAfterAll {

  private val staticContent: String = "static"

  private val staticContentDir: File =
    Files.createTempDirectory("integration-test-static-content").toFile

  override def staticContentConfig: Option[StaticContentConfig] =
    Some(StaticContentConfig(prefix = staticContent, directory = staticContentDir))

  override def jdbcConfig: Option[JdbcConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  private val expectedDummyContent: String = Gen
    .listOfN(100, Gen.identifier)
    .map(_.mkString(" "))
    .sample
    .getOrElse(throw new IllegalStateException(s"Cannot create dummy text content"))

  private val dummyFile: File =
    writeToFile(new File(staticContentDir, "dummy.txt"), expectedDummyContent).get
  require(dummyFile.exists)

  override protected def afterAll(): Unit = {
    // clean up temp directory
    discard { dummyFile.delete() }
    discard { staticContentDir.delete() }
    super.afterAll()
  }

  "should serve static content from configured directory" in withHttpService {
    (uri: Uri, _, _, _) =>
      Http()
        .singleRequest(
          HttpRequest(
            method = HttpMethods.GET,
            uri = uri.withPath(Uri.Path(s"/$staticContent/${dummyFile.getName}")),
          )
        )
        .flatMap { resp =>
          discard { resp.status shouldBe StatusCodes.OK }
          val bodyF: Future[String] = getResponseDataBytes(resp, debug = false)
          bodyF.flatMap { body =>
            body shouldBe expectedDummyContent
          }
        }: Future[Assertion]
  }
}

final class HttpServiceIntegrationTestCustomToken
    extends HttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
