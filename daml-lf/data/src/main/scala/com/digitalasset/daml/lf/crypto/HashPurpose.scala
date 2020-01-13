// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.crypto

class HashPurpose private (val id: Int)

object HashPurpose {
  val ContractKey = new HashPurpose(1)
}
