// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.converter

import com.daml.script.{converter => here}

trait ConverterPackageCompat {
  type ConverterException = here.ConverterException
}
