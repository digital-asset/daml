// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value
package test

@deprecated("1.12.0", "use com.daml.lf.value.Util")
object ValueNormalizer {

  def normalize(
      value0: Value[Value.ContractId],
      version: transaction.TransactionVersion,
  ): Value[Value.ContractId] = {
    Util.normalize(value0, version)
  }

}
