// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import com.digitalasset.ledger.api.v1.value.Identifier

final case class TestTemplateIdentifiers(testPackageId: String) {

  val dummy = Identifier(testPackageId, "Test.Dummy", "Test", "Dummy")
  val dummyWithParam = Identifier(testPackageId, "Test.DummyWithParam", "Test", "DummyWithParam")
  val dummyFactory = Identifier(testPackageId, "Test.DummyFactory", "Test", "DummyFactory")
  val dummyContractFactory =
    Identifier(testPackageId, "Test.DummyContractFactory", "Test", "DummyContractFactory")
  val parameterShowcase =
    Identifier(testPackageId, "Test.ParameterShowcase", "Test", "ParameterShowcase")
  val agreementFactory =
    Identifier(testPackageId, "Test.AgreementFactory", "Test", "AgreementFactory")
  val agreement = Identifier(testPackageId, "Test.Agreement", "Test", "Agreement")
  val triAgreement = Identifier(testPackageId, "Test.TriAgreement", "Test", "TriAgreement")
  val triProposal = Identifier(testPackageId, "Test.TriProposal", "Test", "TriProposal")
  val textContainer = Identifier(testPackageId, "Test.TextContainer", "Test", "TextContainer")
  val nothingArgument = Identifier(testPackageId, "Test.NothingArgument", "Test", "NothingArgument")
  val withObservers = Identifier(testPackageId, "Test.WithObservers", "Test", "WithObservers")
  val branchingSignatories =
    Identifier(testPackageId, "Test.BranchingSignatories", "Test", "BranchingSignatories")
  val branchingControllers =
    Identifier(testPackageId, "Test.BranchingControllers", "Test", "BranchingControllers")
  val divulgence1 =
    Identifier(testPackageId, "Test.Divulgence1", "Test", "Divulgence1")
  val divulgence2 =
    Identifier(testPackageId, "Test.Divulgence2", "Test", "Divulgence2")
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
      divulgence1,
      divulgence2)
}
