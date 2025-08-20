// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.http
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.ValueAddend as VA
import spray.json.JsValue

/** A query, a value that matches the query, and a value that doesn't match.
  */
class FilterDiscriminatorScenario[Inj](
    val label: String,
    val ctId: http.ContractTypeId.Template.RequiredPkg,
    val va: VA.Aux[Inj],
    val query: Map[String, JsValue],
    val matches: Seq[http.Party => Inj],
    val doesNotMatch: Seq[http.Party => Inj],
)

object FilterDiscriminatorScenario {
  def Scenario(
      label: String,
      ctId: http.ContractTypeId.Template.RequiredPkg,
      va: VA,
      query: Map[String, JsValue],
  )(
      matches: Seq[http.Party => va.Inj],
      doesNotMatch: Seq[http.Party => va.Inj],
  ): FilterDiscriminatorScenario[va.Inj] =
    new FilterDiscriminatorScenario(label, ctId, va, query, matches, doesNotMatch)
}
