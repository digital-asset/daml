// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.protocol.{LfContractId, LfHash}

/** mixin for tests that need new, unique LfContractIds (V1-based) */
trait NeedsNewLfContractIds {
  this: BaseTest =>

  val hasher: () => LfHash = LfHash.secureRandom(LfHash.hashPrivateKey(loggerFactory.name))

  def newLfContractId(): LfContractId = LfContractId.V1(hasher(), hasher().bytes)

  def newLfContractIdUnsuffixed(): LfContractId = LfContractId.V1(hasher())

}
