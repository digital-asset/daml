// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

abstract class EnumRef extends ValueRef {
  val constructor: String
  val index: Int
}
