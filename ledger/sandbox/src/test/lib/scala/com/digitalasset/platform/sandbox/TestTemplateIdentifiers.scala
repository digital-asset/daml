// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.digitalasset.ledger.api.v1.value.Identifier

// TODO(mthvedt): Delete this old copy when we finish migrating to ledger-api-integration-tests.
class TestTemplateIdentifiers(testPackageId: String) {

  val dummy =
    Identifier(testPackageId, moduleName = "Test", entityName = "Dummy")
  val dummyWithParam =
    Identifier(testPackageId, moduleName = "Test", entityName = "DummyWithParam")
  val dummyFactory =
    Identifier(testPackageId, moduleName = "Test", entityName = "DummyFactory")
  val dummyContractFactory =
    Identifier(testPackageId, moduleName = "Test", entityName = "DummyContractFactory")
  val parameterShowcase =
    Identifier(testPackageId, moduleName = "Test", entityName = "ParameterShowcase")
  val parameterShowcaseChoice1 =
    Identifier(testPackageId, moduleName = "Test", entityName = "Choice1")
  val optionalInteger =
    Identifier(testPackageId, moduleName = "Test", entityName = "OptionalInteger")
  val agreementFactory =
    Identifier(testPackageId, moduleName = "Test", entityName = "AgreementFactory")
  val agreement =
    Identifier(testPackageId, moduleName = "Test", entityName = "Agreement")
  val triAgreement =
    Identifier(testPackageId, moduleName = "Test", entityName = "TriAgreement")
  val triProposal =
    Identifier(testPackageId, moduleName = "Test", entityName = "TriProposal")
  val textContainer =
    Identifier(testPackageId, moduleName = "Test", entityName = "TextContainer")
  val nothingArgument =
    Identifier(testPackageId, moduleName = "Test", entityName = "NothingArgument")
  val maybeType =
    Identifier(testPackageId, moduleName = "Test", entityName = "Maybe")
  val withObservers =
    Identifier(testPackageId, moduleName = "Test", entityName = "WithObservers")
  val branchingSignatories =
    Identifier(testPackageId, moduleName = "Test", entityName = "BranchingSignatories")
  val branchingControllers =
    Identifier(testPackageId, moduleName = "Test", entityName = "BranchingControllers")
  val callablePayout =
    Identifier(testPackageId, moduleName = "Test", entityName = "CallablePayout")
  val callablePayoutTransfer =
    Identifier(testPackageId, moduleName = "Test", entityName = "Transfer")
  val callablePayoutCall =
    Identifier(testPackageId, moduleName = "Test", entityName = "Call")
  val textKey =
    Identifier(testPackageId, moduleName = "Test", entityName = "TextKey")
  val textKeyOperations =
    Identifier(testPackageId, moduleName = "Test", entityName = "TextKeyOperations")
  val witnesses =
    Identifier(testPackageId, moduleName = "Test", entityName = "Witnesses")
  val allTemplates =
    List(
      dummy,
      dummyWithParam,
      dummyFactory,
      agreement,
      agreementFactory,
      triProposal,
      triAgreement,
      textContainer,
      textKey,
      textKeyOperations,
      witnesses)
}
