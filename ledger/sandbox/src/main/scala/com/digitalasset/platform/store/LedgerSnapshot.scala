// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.platform.store.Contract.ActiveContract

case class LedgerSnapshot(offset: Long, acs: Source[ActiveContract, NotUsed])
