// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_15

import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.model.da.types
import com.daml.ledger.test.java.model.test.{Delegated, Delegation, Dummy, TextKey}

object CompanionImplicits {

  implicit val dummyCompanion
      : ContractCompanion.WithoutKey[Dummy.Contract, Dummy.ContractId, Dummy] = Dummy.COMPANION
  implicit val textKeyCompanion: ContractCompanion.WithKey[
    TextKey.Contract,
    TextKey.ContractId,
    TextKey,
    types.Tuple2[String, String],
  ] = TextKey.COMPANION
  implicit val delegationCompanion
      : ContractCompanion.WithoutKey[Delegation.Contract, Delegation.ContractId, Delegation] =
    Delegation.COMPANION
  implicit val delegatedCompanion: ContractCompanion.WithKey[
    Delegated.Contract,
    Delegated.ContractId,
    Delegated,
    types.Tuple2[String, String],
  ] = Delegated.COMPANION

}
