// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.graphql

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import sangria.parser.QueryParser
import sangria.schema.SchemaChange.DescriptionChange
import sangria.schema.Schema

import scala.io.Source

class GraphQLSchemaSpec extends AnyWordSpec with Matchers {
  // TODO GraphQL sangria needs to be upgrade from 2.x.x to 3.x.x due to fatally colliding parboiled dependency (pekko bumps parboiled to 2.5, causing NoSuchMethodError in this test)
  "The rendered schema" ignore {
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
          s"Schema definition does not match:\n- ${changes.map(_.description).mkString("\n- ")}\n"
        )
      }
    }
  }
}
