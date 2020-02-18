// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.services.time

sealed abstract class TimeProviderType extends Product with Serializable {
  def description: String
}

object TimeProviderType {

  case object Static extends TimeProviderType {
    override lazy val description: String = "static time"
  }

  case object WallClock extends TimeProviderType {
    override lazy val description: String = "wall-clock time"
  }

}
