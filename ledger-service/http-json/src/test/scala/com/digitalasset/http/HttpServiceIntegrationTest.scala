// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Files

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.`X-Forwarded-Proto`
import akka.http.scaladsl.model.{HttpHeader, HttpMethods, HttpRequest, StatusCode, StatusCodes, Uri}
import com.daml.http.HttpServiceTestFixture.LeakPasswords
import com.daml.http.Statement.discard
import com.daml.http.util.TestUtil.writeToFile
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterAll}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class HttpServiceIntegrationTest extends AbstractHttpServiceIntegrationTest with BeforeAndAfterAll {

  private val staticContent: String = "static"

  private val staticContentDir: File =
    Files.createTempDirectory("integration-test-static-content").toFile

  override def staticContentConfig: Option[StaticContentConfig] =
    Some(StaticContentConfig(prefix = staticContent, directory = staticContentDir))

  override def jdbcConfig: Option[JdbcConfig] = None

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

  "should serve static content from configured directory" in withHttpService { (uri: Uri, _, _) =>
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.GET,
          uri = uri.withPath(Uri.Path(s"/$staticContent/${dummyFile.getName}"))))
      .flatMap { resp =>
        discard { resp.status shouldBe StatusCodes.OK }
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = false)
        bodyF.flatMap { body =>
          body shouldBe expectedDummyContent
        }
      }: Future[Assertion]
  }

  "Forwarded" - {
    import Endpoints.Forwarded
    "can 'parse' sample" in {
      Forwarded("for=192.168.0.1;proto=http;by=192.168.0.42").proto should ===(Some("http"))
    }

    "can 'parse' quoted sample" in {
      Forwarded("for=192.168.0.1;proto = \"https\" ;by=192.168.0.42").proto should ===(
        Some("https"))
    }

    import spray.json._, json.JsonProtocol._
    Seq(
      (
        "without header",
        Seq.empty[HttpHeader],
        StatusCodes.Unauthorized: StatusCode,
        "errors" -> Seq(Endpoints.nonHttpsErrorMessage).toJson),
      (
        "with old-style Forwarded",
        Seq(`X-Forwarded-Proto`("https")),
        StatusCodes.OK,
        "result" -> JsArray(Vector())),
      (
        "with new-style Forwarded",
        Seq(Forwarded("for=192.168.0.1;proto = \"https\" ;by=192.168.0.42")),
        StatusCodes.OK,
        "result" -> JsArray(Vector())),
    ) foreach {
      case (lbl, extraHeaders, expectedStatus, expectedOutput) =>
        s"is checked $lbl" in HttpServiceTestFixture.withHttpService(
          testId,
          List.empty,
          jdbcConfig,
          staticContentConfig,
          leakPasswords = LeakPasswords.No) { (uri, _, _, _) =>
          getRequest(
            uri = uri.withPath(Uri.Path("/v1/query")),
            headers = headersWithAuth ++ extraHeaders)
            .map {
              case (status, output) =>
                discard { status shouldBe expectedStatus }
                discard { assertStatus(output, expectedStatus) }
                inside(output) {
                  case JsObject(fields) =>
                    (fields: Iterable[(String, JsValue)]) should contain(expectedOutput)
                }
            }: Future[Assertion]
        }
    }
  }
}
