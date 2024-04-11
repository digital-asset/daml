// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

/** Retries parameter for DB-backed operations.
  *
  * @param maxRetries The maximum number of query retries.
  * @param suspendRetriesOnInactive If set, retries are suspended when the database [[com.digitalasset.canton.resource.Storage]] is inactive (see [[com.digitalasset.canton.resource.DbStorage.run]]).
  *                                 Set to `false` if you want to ensure that `maxRetries` is respected regardless of the storage state.
  */
final case class DbRetries(maxRetries: Int = Forever, suspendRetriesOnInactive: Boolean = true)
