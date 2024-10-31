// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.ledger.api.{LfIdentifier, LfValue}

trait TemplateId[T <: Template] {
  def templateId: LfIdentifier

  def apply(arg: LfValue): T
}
