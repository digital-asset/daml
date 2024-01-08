// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.TransferCounterO

trait HasSerializableContract {

  def contract: SerializableContract

  def transferCounter: TransferCounterO

}
