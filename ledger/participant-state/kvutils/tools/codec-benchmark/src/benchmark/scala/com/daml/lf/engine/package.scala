// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.benchmark.DecodedValueWithType
import com.daml.lf.engine.preprocessing.ValueTranslator
import com.daml.lf.speedy.SValue

package object engine {

  private[engine] def assertTranslate(translator: ValueTranslator)(
      value: DecodedValueWithType,
  ): SValue =
    translator.translateValue(value.valueType, value.value).fold(e => sys.error(e.msg), identity)

}
