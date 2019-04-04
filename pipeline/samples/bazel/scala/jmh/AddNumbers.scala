// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package foo

object AddNumbers {
  def addUntil1000: Int = {
    (0 until 1000).reduceOption(_ + _).getOrElse(0)
  }
}
