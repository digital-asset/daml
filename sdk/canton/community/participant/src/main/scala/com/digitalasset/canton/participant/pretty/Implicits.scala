// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pretty

import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.pretty.Pretty

object Implicits {
  import com.digitalasset.canton.logging.pretty.Pretty.*

  implicit def prettyChangeId: Pretty[ChangeId] = prettyOfClass(
    param("application Id", _.applicationId),
    param("command Id", _.commandId),
    param("act as", _.actAs),
  )

  implicit def prettyContractId: Pretty[ContractId[_]] = prettyOfString { coid =>
    val coidStr = coid.contractId
    val tokens = coidStr.split(':')
    if (tokens.lengthCompare(2) == 0) {
      tokens(0).readableHash.toString + ":" + tokens(1).readableHash.toString
    } else {
      // Don't abbreviate anything for unusual contract ids
      coidStr
    }
  }

  implicit def prettyCompletion: Pretty[Completion] =
    prettyOfClass(
      unnamedParamIfDefined(_.status),
      param("commandId", _.commandId.singleQuoted),
      param("updateId", _.updateId.singleQuoted, _.updateId.nonEmpty),
    )
}
