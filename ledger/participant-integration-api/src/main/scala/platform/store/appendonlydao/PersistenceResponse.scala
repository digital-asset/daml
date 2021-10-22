// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

private[platform] sealed abstract class PersistenceResponse extends Product with Serializable

private[platform] object PersistenceResponse {

  case object Ok extends PersistenceResponse

  case object Duplicate extends PersistenceResponse

}
