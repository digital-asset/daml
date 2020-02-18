// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.services.time

sealed abstract class TimeProviderType extends Product with Serializable

object TimeProviderType {

  def default: Static.type = Static

  case object Static extends TimeProviderType

  case object WallClock extends TimeProviderType

}
