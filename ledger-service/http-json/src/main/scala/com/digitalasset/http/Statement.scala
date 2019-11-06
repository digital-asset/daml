// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

object Statement {
  @specialized def discard[A](evaluateForSideEffectOnly: A): Unit = {
    val _: A = evaluateForSideEffectOnly
    () //Return unit to prevent warning due to discarding value
  }
}
