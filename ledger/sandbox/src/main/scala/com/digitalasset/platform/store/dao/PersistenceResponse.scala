// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

sealed abstract class PersistenceResponse extends Product with Serializable

object PersistenceResponse {

  case object Ok extends PersistenceResponse

  case object Duplicate extends PersistenceResponse

}
