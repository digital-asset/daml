// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scalaz.Equal

private[lf] object ScalazEqual {
  def withNatural[A](isNatural: Boolean)(c: (A, A) => Boolean): Equal[A] =
    if (isNatural) Equal.equalA else Equal.equal(c)
}
