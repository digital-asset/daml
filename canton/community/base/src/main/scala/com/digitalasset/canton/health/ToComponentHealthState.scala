// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

/** Interface that provides conversion from a State type to [[ComponentHealthState]]
  */
trait ToComponentHealthState {

  /** Convert a state to [[ComponentHealthState]]
    */
  def toComponentHealthState: ComponentHealthState
}
