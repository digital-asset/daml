// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark.TypedValue
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value

package object engine {

  private[engine] def assertTranslate(translator: ValueTranslator)(
      value: TypedValue[Value]
  ): SValue =
    translator
      .translateValue(value.valueType, value.value)
      .fold(e => sys.error(e.message), identity)

}
