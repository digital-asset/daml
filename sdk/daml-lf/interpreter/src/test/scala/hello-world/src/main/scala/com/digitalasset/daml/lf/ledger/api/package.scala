// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package ledger.api

import com.digitalasset.daml.lf.ledger.api.{
  LfIdentifier,
  LfValue,
  LfValueContractId,
  LfValueConverterFrom,
  LfValueConverterTo,
}

package object internal {
  private[lf] def createContract(templateId: LfIdentifier, arg: LfValue): LfValueContractId = {
    import LfValueConverterFrom._
    import LfValueConverterTo._

    internal.host
      .createContract(templateId.toByteArray, arg.toByteArray)
      .toLfValue[LfValueContractId]
  }

  private[lf] def fetchContractArg(
      templateId: LfIdentifier,
      contractId: LfValueContractId,
  ): LfValue = {
    import LfValueConverterFrom._
    import LfValueConverterTo._

    internal.host.fetchContractArg(templateId.toByteArray, contractId.toByteArray).toLfValue
  }

  private[lf] def exerciseChoice[R <: LfValue](
      templateId: LfIdentifier,
      contractId: LfValueContractId,
      choiceName: String,
      choiceArg: LfValue,
  ): R = {
    import LfValueConverterFrom._
    import LfValueConverterTo._

    internal.host
      .exerciseChoice(
        templateId.toByteArray,
        contractId.toByteArray,
        choiceName,
        choiceArg.toByteArray,
      )
      .toLfValue[R]
  }
}
