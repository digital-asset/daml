// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import com.digitalasset.ledger.api.v1.value.Identifier

final case class TestTemplateIdentifiers(testPackageId: String) {

  val dummy = Identifier(testPackageId, "Test.Dummy", moduleName = "Test", entityName = "Dummy")
  val dummyWithParam =
    Identifier(
      testPackageId,
      "Test.DummyWithParam",
      moduleName = "Test",
      entityName = "DummyWithParam")
  val dummyFactory =
    Identifier(testPackageId, "Test.DummyFactory", moduleName = "Test", entityName = "DummyFactory")
  val dummyContractFactory =
    Identifier(
      testPackageId,
      "Test.DummyContractFactory",
      moduleName = "Test",
      entityName = "DummyContractFactory")
  val parameterShowcase =
    Identifier(
      testPackageId,
      "Test.ParameterShowcase",
      moduleName = "Test",
      entityName = "ParameterShowcase")
  val parameterShowcaseChoice1 =
    Identifier(testPackageId, "Test.Choice1", moduleName = "Test", entityName = "Choice1")
  val optionalInteger =
    Identifier(
      testPackageId,
      "Test.OptionalInteger",
      moduleName = "Test",
      entityName = "OptionalInteger")
  val agreementFactory =
    Identifier(
      testPackageId,
      "Test.AgreementFactory",
      moduleName = "Test",
      entityName = "AgreementFactory")
  val agreement =
    Identifier(testPackageId, "Test.Agreement", moduleName = "Test", entityName = "Agreement")
  val triAgreement =
    Identifier(testPackageId, "Test.TriAgreement", moduleName = "Test", entityName = "TriAgreement")
  val triProposal =
    Identifier(testPackageId, "Test.TriProposal", moduleName = "Test", entityName = "TriProposal")
  val textContainer = Identifier(
    testPackageId,
    "Test.TextContainer",
    moduleName = "Test",
    entityName = "TextContainer")
  val nothingArgument =
    Identifier(
      testPackageId,
      "Test.NothingArgument",
      moduleName = "Test",
      entityName = "NothingArgument")
  val maybeType = Identifier(testPackageId, "Test.Maybe", moduleName = "Test", entityName = "Maybe")
  val withObservers = Identifier(
    testPackageId,
    "Test.WithObservers",
    moduleName = "Test",
    entityName = "WithObservers")
  val branchingSignatories =
    Identifier(
      testPackageId,
      "Test.BranchingSignatories",
      moduleName = "Test",
      entityName = "BranchingSignatories")
  val branchingControllers =
    Identifier(
      testPackageId,
      "Test.BranchingControllers",
      moduleName = "Test",
      entityName = "BranchingControllers")
  val callablePayout =
    Identifier(
      testPackageId,
      "Test.CallablePayout",
      moduleName = "Test",
      entityName = "CallablePayout")
  val callablePayoutTransfer =
    Identifier(testPackageId, "Test.Transfer", moduleName = "Test", entityName = "Transfer")
  val callablePayoutCall =
    Identifier(testPackageId, "Test.Call", moduleName = "Test", entityName = "Call")
  val textKey =
    Identifier(testPackageId, "Test.TextKey", moduleName = "Test", entityName = "TextKey")
  val textKeyOperations =
    Identifier(
      testPackageId,
      "Test.TextKeyOperations",
      moduleName = "Test",
      entityName = "TextKeyOperations")
  val divulgence1 =
    Identifier(testPackageId, "Test.Divulgence1", "Test", "Divulgence1")
  val divulgence2 =
    Identifier(testPackageId, "Test.Divulgence2", "Test", "Divulgence2")
  val decimalRounding =
    Identifier(testPackageId, "Test.DecimalRounding", "Test", "DecimalRounding")
  val delegated = Identifier(testPackageId, moduleName = "Test", entityName = "Delegated")
  val delegation = Identifier(testPackageId, moduleName = "Test", entityName = "Delegation")
  val showDelegated =
    Identifier(testPackageId, moduleName = "Test", entityName = "ShowDelegated")
  val witnesses =
    Identifier(testPackageId, "Test.Witnesses", moduleName = "Test", entityName = "Witnesses")
  val divulgeWitnesses =
    Identifier(
      testPackageId,
      "Test.DivulgeWitnesses",
      moduleName = "Test",
      entityName = "DivulgeWitnesses")
  val maintainerNotSignatory =
    Identifier(
      testPackageId,
      "Test.MaintainerNotSignatory",
      moduleName = "Test",
      entityName = "MaintainerNotSignatory")
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
      divulgence1,
      divulgence2,
      witnesses,
      maintainerNotSignatory
    )
}
