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

class JsonApiReferenceDocsTest extends AnyWordSpecLike with BaseTest with Checkpoints {
  private val RootTestResources =
    "community/ledger/ledger-json-api/src/test/resources/json-api-docs"

  private val ProtoExtractedData =
    s"community/ledger/ledger-json-api/src/main/resources/${ProtoInfo.LedgerApiDescriptionResourceLocation}"

  private val OpenApiYaml = s"$RootTestResources/openapi.yaml"
  private val AsyncApiYaml = s"$RootTestResources/asyncapi.yaml"
  private val apiDocsGenerator = new ApiDocsGenerator(NamedLoggerFactory.root)

  "ProtoParser" should {
    // TODO(#21695): Test only used to generate golden files.
    //               Ignore once stable
    "output proto extracted data" in {
      // Unignore to regenerate proto descriptions
      val protoData = ProtoParser.readProto()
      File(ProtoExtractedData)
        .createFileIfNotExists()
        .overwrite(protoData.toYaml())
        .discard
    }
  }

  "Canton JSON API v2" should {
    // TODO(#21695): Test only used to generate golden files.
    //               Ignore once stable
    "output the definitions of the golden DAR" in {
      val apiDocs = apiDocsGenerator.createStaticDocs()
      File(OpenApiYaml)
        .createFileIfNotExists()
        .overwrite(apiDocs.openApi)
        .discard

      File(AsyncApiYaml)
        .createFileIfNotExists()
        .overwrite(apiDocs.asyncApi)
        .discard
    }

    "validate the definitions in the golden DAR against the golden files" in {
      def failureClue(docsType: String): String =
        s"""Current $docsType definitions do not match the golden file.
          | Overwrite the golden file with the current definitions if the API changed.
          | Definitions can be updated by unignoring both tests above.""".stripMargin

      val apiDocs = apiDocsGenerator.createStaticDocs()
      val openApi = File(OpenApiYaml).contentAsString
      val asyncApi = File(AsyncApiYaml).contentAsString

      apiDocs.openApi shouldBe openApi withClue failureClue("OpenAPI")
      apiDocs.asyncApi shouldBe asyncApi withClue failureClue("AsyncAPI")
    }
  }
}
