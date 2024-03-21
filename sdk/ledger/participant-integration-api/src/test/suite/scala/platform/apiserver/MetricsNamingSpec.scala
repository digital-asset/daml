// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class MetricsNamingSpec extends AnyFlatSpec with Matchers {

  behavior of "MetricsNaming.camelCaseToSnakeCase"

  import MetricsNaming.camelCaseToSnakeCase

  it should "leave an empty string unchanged" in {
    camelCaseToSnakeCase("") shouldBe ""
  }

  it should "leave a snake_cased string unchanged" in {
    camelCaseToSnakeCase("snake_cased") shouldBe "snake_cased"
  }

  it should "remove the capitalization of the first letter" in {
    camelCaseToSnakeCase("Camel") shouldBe "camel"
  }

  it should "turn a single capital letter into a an underscore followed by a lower case letter" in {
    camelCaseToSnakeCase("CamelCase") shouldBe "camel_case"
    camelCaseToSnakeCase("camelCase") shouldBe "camel_case"
  }

  it should "keep acronyms together and change their capitalization as a single unit" in {
    camelCaseToSnakeCase("ACRONYM") shouldBe "acronym"
    camelCaseToSnakeCase("ACRONYMFactory") shouldBe "acronym_factory"
    camelCaseToSnakeCase("AbstractACRONYM") shouldBe "abstract_acronym"
    camelCaseToSnakeCase("AbstractACRONYMFactory") shouldBe "abstract_acronym_factory"
    camelCaseToSnakeCase(
      "AbstractACRONYMProxyJVMFactory"
    ) shouldBe "abstract_acronym_proxy_jvm_factory"
  }

  it should "treat single letter words intelligently" in {
    camelCaseToSnakeCase("ATeam") shouldBe "a_team"
    camelCaseToSnakeCase("TeamA") shouldBe "team_a"
    camelCaseToSnakeCase("BustAMove") shouldBe "bust_a_move"

    // the following is mostly to document a reasonable short-coming:
    // a single letter word followed by an acronym will be detected as a single acronym
    camelCaseToSnakeCase("AJVMHeap") shouldBe "ajvm_heap"
  }

  behavior of "MetricsNaming.nameFor"

  import MetricsNaming.nameFor

  it should "produce the expected name for a selection of service methods" in {
    nameFor(
      CommandServiceGrpc.METHOD_SUBMIT_AND_WAIT.getFullMethodName
    ).toString shouldBe "command_service.submit_and_wait"
    nameFor(
      CommandSubmissionServiceGrpc.METHOD_SUBMIT.getFullMethodName
    ).toString shouldBe "command_submission_service.submit"
    nameFor(
      ActiveContractsServiceGrpc.METHOD_GET_ACTIVE_CONTRACTS.getFullMethodName
    ).toString shouldBe "active_contracts_service.get_active_contracts"
  }

}
