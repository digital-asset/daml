// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy
package wasm

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value.VersionedContractInstance
import com.digitalasset.daml.lf.value.{Value => LfValue}

import scala.concurrent.{ExecutionContext, Future}

object WasmRunnerTestLib {

  def acs(
      getContract: PartialFunction[LfValue.ContractId, LfValue.VersionedContractInstance] =
        PartialFunction.empty
  )(implicit ec: ExecutionContext): ParticipantContractStore = new ParticipantContractStore {
    override def lookupActiveContract(readers: Set[Party], contractId: LfValue.ContractId)(implicit
        loggingContext: LoggingContext
    ): Future[Option[VersionedContractInstance]] = Future {
      getContract.lift(contractId)
    }
  }

}
