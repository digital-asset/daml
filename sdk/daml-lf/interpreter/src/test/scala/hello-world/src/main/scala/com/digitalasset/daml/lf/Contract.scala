// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.ledger.api.{LfValue, LfValueContractId}

class Contract[T <: Template, CT <: TemplateId[T]](val contractId: LfValueContractId)(implicit
    companion: CT
) {
  def fetch(): T = {
    val arg = ledger.api.internal.fetchContractArg(companion.templateId, contractId)

    companion.apply(arg)
  }

  def exercise[In <: LfValue, Out <: LfValue](choiceName: String, arg: In): Out = {
    ledger.api.internal.exerciseChoice[Out](companion.templateId, contractId, choiceName, arg)
  }
}
