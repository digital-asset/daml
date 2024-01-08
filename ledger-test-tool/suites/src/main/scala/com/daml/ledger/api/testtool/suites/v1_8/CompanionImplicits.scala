// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.model.da.types
import com.daml.ledger.test.java.model.iou.Iou
import com.daml.ledger.test.java.model.test
import com.daml.ledger.test.java.model.test.{
  Agreement,
  AgreementFactory,
  CallablePayout,
  Delegated,
  Delegation,
  Divulgence1,
  Divulgence2,
  Dummy,
  DummyFactory,
  DummyWithParam,
  LocalKeyVisibilityOperations,
  MaintainerNotSignatory,
  ShowDelegated,
  TextKey,
  TextKeyOperations,
  WithObservers,
  Witnesses => TestWitnesses,
}
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
  implicit val textKeyCompanion: ContractCompanion.WithKey[
    TextKey.Contract,
    TextKey.ContractId,
    TextKey,
    types.Tuple2[String, String],
  ] = TextKey.COMPANION
  implicit val textKeyOperationsCompanion: ContractCompanion.WithoutKey[
    TextKeyOperations.Contract,
    TextKeyOperations.ContractId,
    TextKeyOperations,
  ] = TextKeyOperations.COMPANION
  implicit val callablePayoutCompanion: ContractCompanion.WithoutKey[
    CallablePayout.Contract,
    CallablePayout.ContractId,
    CallablePayout,
  ] = CallablePayout.COMPANION
  implicit val delegatedCompanion: ContractCompanion.WithKey[
    Delegated.Contract,
    Delegated.ContractId,
    Delegated,
    types.Tuple2[String, String],
  ] = Delegated.COMPANION
  implicit val delegationCompanion
      : ContractCompanion.WithoutKey[Delegation.Contract, Delegation.ContractId, Delegation] =
    Delegation.COMPANION
  implicit val showDelegatedCompanion: ContractCompanion.WithoutKey[
    ShowDelegated.Contract,
    ShowDelegated.ContractId,
    ShowDelegated,
  ] = ShowDelegated.COMPANION
  implicit val maintainerNotSignatoryCompanion: ContractCompanion.WithKey[
    MaintainerNotSignatory.Contract,
    MaintainerNotSignatory.ContractId,
    MaintainerNotSignatory,
    String,
  ] = MaintainerNotSignatory.COMPANION
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
  implicit val localKeyVisibilityOperationsCompanion: ContractCompanion.WithoutKey[
    LocalKeyVisibilityOperations.Contract,
    LocalKeyVisibilityOperations.ContractId,
    LocalKeyVisibilityOperations,
  ] = LocalKeyVisibilityOperations.COMPANION
  implicit val withKeyCompanion: ContractCompanion.WithKey[
    test.WithKey.Contract,
    test.WithKey.ContractId,
    test.WithKey,
    String,
  ] = test.WithKey.COMPANION
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

}
