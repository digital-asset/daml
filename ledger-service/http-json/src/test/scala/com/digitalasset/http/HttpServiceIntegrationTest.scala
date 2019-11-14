// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.io.File

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, StatusCodes, Uri}
import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.http.Statement.discard
import com.digitalasset.http.util.TestUtil.requiredFile
import org.scalatest.Assertion

import scala.concurrent.Future
import scala.io.Source

class HttpServiceIntegrationTest extends AbstractHttpServiceIntegrationTest {

  private val staticContentDir =
    requiredFile(rlocation("ledger-service/http-json/src/test/resources/static-content-dir"))
      .fold(e => throw new IllegalStateException(e), identity)

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] =
    Some(StaticContentConfig(prefix = "static", directory = staticContentDir))

  private val dummyFile = new File(staticContentDir, "dummy.txt")
  require(dummyFile.exists)

  private def expectedDummyContent: String = {
    val buffer = Source.fromFile(dummyFile)
    try {
      buffer.mkString
    } finally {
      buffer.close
    }
  }

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
