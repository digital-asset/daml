// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.services.time

sealed abstract class TimeProviderType extends Product with Serializable
object TimeProviderType {

  case object Static extends TimeProviderType
  case object StaticAllowBackwards extends TimeProviderType
  case object WallClock extends TimeProviderType

  def default: Static.type = Static

}
