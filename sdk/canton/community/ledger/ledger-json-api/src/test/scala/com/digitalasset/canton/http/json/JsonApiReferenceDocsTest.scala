// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import better.files.File
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.v2.{ApiDocsGenerator, ProtoInfo}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.proto.ProtoParser
import org.scalatest.Checkpoints
import org.scalatest.wordspec.AnyWordSpecLike

/** We test if the openapi and asyncapi definitions are not changed. If so files should be updated
  * (by calling GenerateJSONApiDocs.regenerateAll())
  *
  * Notice: changes to .proto files do not cause this test to fail, as we use store extracted
  * information (in proto-data.yml).
  *
  * This is a deliberate design choice to avoid noise / too many accidental build fails on minor
  * changes in proto comments.
  */
class JsonApiReferenceDocsTest extends AnyWordSpecLike with BaseTest with Checkpoints {

  private val apiDocsGenerator = new ApiDocsGenerator(NamedLoggerFactory.root)
  private val existingOpenApi = File(JsonApiReferenceDocsTest.OpenApiYaml).contentAsString
  private val existingAsyncApi = File(JsonApiReferenceDocsTest.AsyncApiYaml).contentAsString

  "Canton JSON API v2" should {
    "output golden openapi definitions " ignore {
      GenerateJSONApiDocs.regenerateAll()
    }

    "validate the definitions against the golden files" in {
      def failureClue(docsType: String): String =
        s"""Current $docsType definitions do not match the golden file.
          | Overwrite the golden file with the current definitions if the API changed.
          | Definitions can be updated by running GenerateJSONApiDocs class.""".stripMargin
      val protoInfo = apiDocsGenerator.loadProtoData()
      val apiDocs = apiDocsGenerator.createStaticDocs(protoInfo)

      apiDocs.openApi shouldBe existingOpenApi withClue failureClue("OpenAPI")
      apiDocs.asyncApi shouldBe existingAsyncApi withClue failureClue("AsyncAPI")
    }
  }
}

object JsonApiReferenceDocsTest {
  private val RootTestResources =
    "community/ledger/ledger-json-api/src/test/resources/json-api-docs"

  val ProtoExtractedData =
    s"community/ledger/ledger-json-api/src/main/resources/${ProtoInfo.LedgerApiDescriptionResourceLocation}"

  val OpenApiYaml = s"$RootTestResources/openapi.yaml"
  val AsyncApiYaml = s"$RootTestResources/asyncapi.yaml"

}

/**   1. extract proto-data.yml from proto files 2. using proto-data.yml regenerate openapi and
  *      asyncapi files
  */
object GenerateJSONApiDocs extends App {
  private lazy val apiDocsGenerator = new ApiDocsGenerator(NamedLoggerFactory.root)

  regenerateAll()

  def regenerateAll() = {
    val protoInfo = regenerateProtoData()
    regenerateJsonApi(protoInfo)
  }

  def regenerateProtoData() = {
    val protoData = ProtoParser.readProto()
    File(JsonApiReferenceDocsTest.ProtoExtractedData)
      .createFileIfNotExists()
      .overwrite(protoData.toYaml())
      .discard
    ProtoInfo(protoData)
  }
  def regenerateJsonApi(protoData: ProtoInfo) = {
    val apiDocs = apiDocsGenerator.createStaticDocs(protoData)
    File(JsonApiReferenceDocsTest.OpenApiYaml)
      .createFileIfNotExists()
      .overwrite(apiDocs.openApi)
      .discard

    File(JsonApiReferenceDocsTest.AsyncApiYaml)
      .createFileIfNotExists()
      .overwrite(apiDocs.asyncApi)
      .discard
  }
}
