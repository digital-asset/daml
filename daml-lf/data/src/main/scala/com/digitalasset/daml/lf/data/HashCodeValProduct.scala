// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.runtime.ScalaRunTime

trait HashCodeValProduct extends Product with Serializable {

  private var hashCode_ = 0
  override def hashCode(): Int = {
    if (hashCode_ == 0) {
      val hc = ScalaRunTime._hashCode(this)
      hashCode_ = if (hc != 0) hc else 1
    }
    hashCode_
  }

}

