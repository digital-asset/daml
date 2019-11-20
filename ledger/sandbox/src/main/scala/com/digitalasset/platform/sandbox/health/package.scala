// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

package object health {
  def dropRepeated[T]: DropRepeated[T] = new DropRepeated[T]()
}
