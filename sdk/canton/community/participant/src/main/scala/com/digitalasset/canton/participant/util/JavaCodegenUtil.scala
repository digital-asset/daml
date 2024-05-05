// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import com.daml.ledger.javaapi.data.codegen.ContractId
import com.digitalasset.canton.protocol.LfContractId

object JavaCodegenUtil {
  implicit class ContractIdSyntax[T](contractId: ContractId[?]) {
    def toLf: LfContractId = LfContractId.assertFromString(contractId.contractId)
  }
}
