// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.wasm

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.{Value => LfValue}

import scala.concurrent.Future

trait ParticipantContractStore {

  def lookupActiveContract(
      readers: Set[Ref.Party],
      contractId: LfValue.ContractId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[LfValue.VersionedContractInstance]]
}
