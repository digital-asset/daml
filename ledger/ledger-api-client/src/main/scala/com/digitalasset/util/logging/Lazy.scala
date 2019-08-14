// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.util.logging

/**
  * To be used with slf4j log calls, if the caller wants to avoid both creating lazy vals
  * and the logger.isMyLogLevelEnabled boilerplate.
  */
class Lazy[T](computation: => T) {

  override def toString: String = computation.toString
}

object Lazy {
  def apply[T](computation: => T) = new Lazy(computation)
}
