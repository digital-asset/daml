// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphql

import org.scalatest.{Matchers, WordSpec}
import sangria.parser.QueryParser
import sangria.schema.SchemaChange.DescriptionChange
import sangria.schema.Schema

import scala.io.Source

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class GraphQLSchemaSpec extends WordSpec with Matchers {
  "The rendered schema" should {
    "match the expected schema definition" in {
      val idl =
        Source.fromInputStream(getClass.getResourceAsStream("/schema.graphql"), "UTF-8").mkString
      val schema = Schema.buildFromAst(QueryParser.parse(idl).get)

      // Compare schemata but ignore description changes.
      val changes = schema
        .compare(new GraphQLSchema(Set()).QuerySchema)
        .filter(!_.isInstanceOf[DescriptionChange])

      if (changes.nonEmpty) {
        fail(
          s"Schema definition does not match:\n- ${changes.map(_.description).mkString("\n- ")}\n")
      }
    }
  }
}
