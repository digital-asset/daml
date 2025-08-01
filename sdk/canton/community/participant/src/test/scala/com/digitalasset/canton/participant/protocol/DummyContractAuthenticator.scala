// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.syntax.either.*
import com.digitalasset.canton.protocol.{ContractMetadata, GenContractInstance}
import com.digitalasset.daml.lf.transaction.FatContractInstance

object DummyContractAuthenticator extends ContractAuthenticator {
  override def authenticate(contract: FatContractInstance): Either[String, Unit] = Either.unit
  override def verifyMetadata(
      contract: GenContractInstance,
      metadata: ContractMetadata,
  ): Either[String, Unit] = Either.unit

}
