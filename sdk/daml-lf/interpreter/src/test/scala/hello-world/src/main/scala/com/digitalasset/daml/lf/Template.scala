// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.ledger.api.{LfValue, LfValueParty}

trait Template {

  def arg: LfValue

  def create[T <: Template, CT <: TemplateId[T]]()(implicit companion: CT): Contract[T, CT] = {
    val contractId = ledger.api.internal.createContract(companion.templateId, arg)

    new Contract[T, CT](contractId)
  }

  def precond: Boolean = {
    true
  }

  def signatories: Seq[LfValueParty]

  def observers: Seq[LfValueParty] = {
    Seq.empty
  }
}
