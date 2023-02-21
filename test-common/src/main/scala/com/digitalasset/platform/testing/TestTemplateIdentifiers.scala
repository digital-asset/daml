// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.testing

import com.daml.ledger.api.v1.value.Identifier

final class TestTemplateIdentifiers(testPackageId: String) {

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
  val divulgence1 =
    Identifier(testPackageId, "Test", "Divulgence1")
  val divulgence2 =
    Identifier(testPackageId, "Test", "Divulgence2")
  val decimalRounding =
    Identifier(testPackageId, "Test", "DecimalRounding")
  val delegated =
    Identifier(testPackageId, moduleName = "Test", entityName = "Delegated")
  val delegation =
    Identifier(testPackageId, moduleName = "Test", entityName = "Delegation")
  val showDelegated =
    Identifier(testPackageId, moduleName = "Test", entityName = "ShowDelegated")
  val witnesses =
    Identifier(testPackageId, moduleName = "Test", entityName = "Witnesses")
  val divulgeWitnesses =
    Identifier(testPackageId, moduleName = "Test", entityName = "DivulgeWitnesses")
  val maintainerNotSignatory =
    Identifier(testPackageId, moduleName = "Test", entityName = "MaintainerNotSignatory")
  val createAndFetch =
    Identifier(testPackageId, "Test", "CreateAndFetch")
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
      maintainerNotSignatory,
      createAndFetch,
    )
}
