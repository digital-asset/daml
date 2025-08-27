// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.daml.lf.transaction.FatContractInstance

object ContractAuthenticators {
  // TODO(#27344) - Remove this type in preference of a trait
  type ContractAuthenticatorFn = (FatContractInstance, LfHash) => Either[String, Unit]
}
