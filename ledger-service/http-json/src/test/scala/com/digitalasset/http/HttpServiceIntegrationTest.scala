// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.io.File

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes, Uri}
import com.digitalasset.http.Statement.discard
import com.digitalasset.http.util.TestUtil.{createDirectory, writeToFile}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Assertion

import scala.concurrent.Future

class HttpServiceIntegrationTest extends AbstractHttpServiceIntegrationTest {

  private val staticContentDir = createDirectory("integration-test-static-content")
    .fold(e => throw new IllegalStateException(e), identity)

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] =
    Some(StaticContentConfig(prefix = "static", directory = staticContentDir))

  private val expectedDummyContent: String = Gen
    .listOfN(100, Arbitrary.arbitrary[String])
    .map(_.mkString(" "))
    .sample
    .getOrElse(throw new IllegalStateException(s"Cannot create dummy text content"))

  private val dummyFile: File =
    writeToFile(new File(staticContentDir, "dummy.txt"), expectedDummyContent)
      .fold(e => throw new IllegalStateException(e), identity)
  require(dummyFile.exists)

  "should serve static content from configured directory" in withHttpService { (uri: Uri, _, _) =>
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.GET,
          uri = uri.withPath(Uri.Path(s"/static/${dummyFile.getName}"))))
      .flatMap { resp =>
        discard { resp.status shouldBe StatusCodes.OK }
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = false)
        bodyF.flatMap { body =>
          body shouldBe expectedDummyContent
        }
      }: Future[Assertion]
  }
}
