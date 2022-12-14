// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.lf.value.{Value => LfValue}
import com.daml.ledger.javaapi.data.Value

object ValueConversion {
  def toLfValue(v: Value): LfValue = ???

  def fromLfValue(lfV: LfValue): Value = ???
}
