// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
import spray.json.JsValue

/** A query, a value that matches the query, and a value that doesn't match.
  */
class FilterDiscriminatorScenario[Inj](
    val label: String,
    val ctId: domain.ContractTypeId.Template.OptionalPkg,
    val va: VA.Aux[Inj],
    val query: Map[String, JsValue],
    val matches: Seq[domain.Party => Inj],
    val doesNotMatch: Seq[domain.Party => Inj],
)

object FilterDiscriminatorScenario {
  def Scenario(
      label: String,
      ctId: domain.ContractTypeId.Template.OptionalPkg,
      va: VA,
      query: Map[String, JsValue],
  )(
      matches: Seq[domain.Party => va.Inj],
      doesNotMatch: Seq[domain.Party => va.Inj],
  ): FilterDiscriminatorScenario[va.Inj] =
    new FilterDiscriminatorScenario(label, ctId, va, query, matches, doesNotMatch)
}
