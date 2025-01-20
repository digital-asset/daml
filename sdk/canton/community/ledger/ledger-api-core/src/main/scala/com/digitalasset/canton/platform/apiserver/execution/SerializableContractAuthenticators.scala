// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.digitalasset.canton.platform.apiserver.execution.SerializableContractAuthenticators.AuthenticateSerializableContract
import com.digitalasset.canton.protocol.SerializableContract

object SerializableContractAuthenticators {
  type AuthenticateSerializableContract = SerializableContract => Either[String, Unit]
}

final case class SerializableContractAuthenticators(
    input: AuthenticateSerializableContract,
    upgrade: AuthenticateSerializableContract,
)
