// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.experimental.da.types
import com.daml.ledger.test.java.experimental.test.{
  Delegated,
  Delegation,
  LocalKeyVisibilityOperations,
  MaintainerNotSignatory,
  ShowDelegated,
  TextKey,
  TextKeyOperations,
  WithKey,
}
import com.daml.ledger.test.java.model.test.{CallablePayout, Dummy}

object ContractKeysCompanionImplicits {

  implicit val dummyCompanion
      : ContractCompanion.WithoutKey[Dummy.Contract, Dummy.ContractId, Dummy] = Dummy.COMPANION
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
  implicit val localKeyVisibilityOperationsCompanion: ContractCompanion.WithoutKey[
    LocalKeyVisibilityOperations.Contract,
    LocalKeyVisibilityOperations.ContractId,
    LocalKeyVisibilityOperations,
  ] = LocalKeyVisibilityOperations.COMPANION
  implicit val withKeyCompanion: ContractCompanion.WithKey[
    WithKey.Contract,
    WithKey.ContractId,
    WithKey,
    String,
  ] = WithKey.COMPANION
}
