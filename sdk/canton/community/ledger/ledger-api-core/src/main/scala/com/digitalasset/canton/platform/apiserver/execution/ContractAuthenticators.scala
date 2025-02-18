// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.daml.lf.transaction.FatContractInstance

object ContractAuthenticators {
  type AuthenticateSerializableContract = SerializableContract => Either[String, Unit]
  type AuthenticateFatContractInstance = FatContractInstance => Either[String, Unit]
}
