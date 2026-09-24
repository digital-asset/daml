// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.model.iou.Iou
import com.daml.ledger.test.java.model.test.{
  Agreement,
  AgreementFactory,
  CallablePayout,
  Delegated,
  Delegation,
  DiscloseCreate,
  Divulgence1,
  Divulgence2,
  Dummy,
  DummyFactory,
  DummyWithParam,
  TriProposal,
  WithObservers,
  Witnesses as TestWitnesses,
}
import com.daml.ledger.test.java.model.trailingnones.TrailingNones
import com.daml.ledger.test.java.semantic.divulgencetests.DummyFlexibleController
import com.daml.ledger.test.java.semantic.semantictests

object CompanionImplicits {

  implicit val dummyCompanion
      : ContractCompanion.WithoutKey[Dummy.Contract, Dummy.ContractId, Dummy] = Dummy.COMPANION
  implicit val dummyWithParamCompanion: ContractCompanion.WithoutKey[
    DummyWithParam.Contract,
    DummyWithParam.ContractId,
    DummyWithParam,
  ] = DummyWithParam.COMPANION
  implicit val dummyFactoryCompanion
      : ContractCompanion.WithoutKey[DummyFactory.Contract, DummyFactory.ContractId, DummyFactory] =
    DummyFactory.COMPANION
  implicit val withObserversCompanion: ContractCompanion.WithoutKey[
    WithObservers.Contract,
    WithObservers.ContractId,
    WithObservers,
  ] = WithObservers.COMPANION
  implicit val callablePayoutCompanion: ContractCompanion.WithoutKey[
    CallablePayout.Contract,
    CallablePayout.ContractId,
    CallablePayout,
  ] = CallablePayout.COMPANION
  implicit val delegatedCompanion: ContractCompanion.WithoutKey[
    Delegated.Contract,
    Delegated.ContractId,
    Delegated,
  ] = Delegated.COMPANION
  implicit val delegationCompanion
      : ContractCompanion.WithoutKey[Delegation.Contract, Delegation.ContractId, Delegation] =
    Delegation.COMPANION
  implicit val discloseCreatedCompanion: ContractCompanion.WithoutKey[
    DiscloseCreate.Contract,
    DiscloseCreate.ContractId,
    DiscloseCreate,
  ] = DiscloseCreate.COMPANION
  implicit val testWitnessesCompanion: ContractCompanion.WithoutKey[
    TestWitnesses.Contract,
    TestWitnesses.ContractId,
    TestWitnesses,
  ] = TestWitnesses.COMPANION
  implicit val divulgence1Companion
      : ContractCompanion.WithoutKey[Divulgence1.Contract, Divulgence1.ContractId, Divulgence1] =
    Divulgence1.COMPANION
  implicit val divulgence2Companion
      : ContractCompanion.WithoutKey[Divulgence2.Contract, Divulgence2.ContractId, Divulgence2] =
    Divulgence2.COMPANION
  implicit val semanticTestsIouCompanion: ContractCompanion.WithoutKey[
    semantictests.Iou.Contract,
    semantictests.Iou.ContractId,
    semantictests.Iou,
  ] = semantictests.Iou.COMPANION
  implicit val iouCompanion: ContractCompanion.WithoutKey[Iou.Contract, Iou.ContractId, Iou] =
    Iou.COMPANION
  implicit val agreementFactoryCompanion: ContractCompanion.WithoutKey[
    AgreementFactory.Contract,
    AgreementFactory.ContractId,
    AgreementFactory,
  ] = AgreementFactory.COMPANION
  implicit val agreementCompanion
      : ContractCompanion.WithoutKey[Agreement.Contract, Agreement.ContractId, Agreement] =
    Agreement.COMPANION

  implicit val divulgeIouByExerciseCompanion: ContractCompanion.WithoutKey[
    DummyFlexibleController.Contract,
    DummyFlexibleController.ContractId,
    DummyFlexibleController,
  ] = DummyFlexibleController.COMPANION

  implicit val triProposalCompanion
      : ContractCompanion.WithoutKey[TriProposal.Contract, TriProposal.ContractId, TriProposal] =
    TriProposal.COMPANION

  implicit val trailingNonesCompanion: ContractCompanion.WithoutKey[
    TrailingNones.Contract,
    TrailingNones.ContractId,
    TrailingNones,
  ] =
    TrailingNones.COMPANION
}
