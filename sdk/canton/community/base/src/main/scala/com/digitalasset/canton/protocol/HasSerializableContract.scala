// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.ReassignmentCounter

trait HasSerializableContract {

  def contract: SerializableContract

  def reassignmentCounter: ReassignmentCounter

}
