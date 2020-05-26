// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.services.time

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
