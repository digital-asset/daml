// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

private[platform] sealed abstract class PersistenceResponse extends Product with Serializable

private[platform] object PersistenceResponse {

  case object Ok extends PersistenceResponse

}
