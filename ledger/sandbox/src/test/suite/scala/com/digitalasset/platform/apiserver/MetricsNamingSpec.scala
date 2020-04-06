// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import org.scalatest.{FlatSpec, Matchers}

final class MetricsNamingSpec extends FlatSpec with Matchers {

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
    camelCaseToSnakeCase("DAML") shouldBe "daml"
    camelCaseToSnakeCase("DAMLFactory") shouldBe "daml_factory"
    camelCaseToSnakeCase("AbstractDAML") shouldBe "abstract_daml"
    camelCaseToSnakeCase("AbstractDAMLFactory") shouldBe "abstract_daml_factory"
    camelCaseToSnakeCase("AbstractDAMLProxyJVMFactory") shouldBe "abstract_daml_proxy_jvm_factory"
  }

  it should "treat single letter words intelligently" in {
    camelCaseToSnakeCase("ATeam") shouldBe "a_team"
    camelCaseToSnakeCase("TeamA") shouldBe "team_a"
    camelCaseToSnakeCase("BustAMove") shouldBe "bust_a_move"

    // the following is mostly to document a reasonable short-coming:
    // a single letter word followed by an acronym will be detected as a single acronym
    camelCaseToSnakeCase("AJVMHeap") shouldBe "ajvm_heap"
  }

  behavior of "MetricsNaming.nameForService"

  import MetricsNaming.nameForService

  behavior of "MetricsNaming.nameFor"

  it should "produce the expected name for a selection of services" in {
    nameForService(CommandServiceGrpc.javaDescriptor.getFullName).toString shouldBe "daml.lapi.command_service"
    nameForService(CommandSubmissionServiceGrpc.javaDescriptor.getFullName).toString shouldBe "daml.lapi.command_submission_service"
    nameForService(ActiveContractsServiceGrpc.javaDescriptor.getFullName).toString shouldBe "daml.lapi.active_contracts_service"
  }

  import MetricsNaming.nameFor

  it should "produce the expected name for a selection of service methods" in {
    nameFor(CommandServiceGrpc.METHOD_SUBMIT_AND_WAIT.getFullMethodName).toString shouldBe "daml.lapi.command_service.submit_and_wait"
    nameFor(CommandSubmissionServiceGrpc.METHOD_SUBMIT.getFullMethodName).toString shouldBe "daml.lapi.command_submission_service.submit"
    nameFor(ActiveContractsServiceGrpc.METHOD_GET_ACTIVE_CONTRACTS.getFullMethodName).toString shouldBe "daml.lapi.active_contracts_service.get_active_contracts"
  }

}
