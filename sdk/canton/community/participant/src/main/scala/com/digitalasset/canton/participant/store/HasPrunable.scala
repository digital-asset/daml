// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

trait HasPrunable {

  /** Returns whether pruning may delete an item (contract, key, ... with this state */
  def prunable: Boolean
}
