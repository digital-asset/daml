// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.authentication.grpc

import com.digitalasset.canton.synchronizer.sequencing.authentication.StoredAuthenticationToken
import com.digitalasset.canton.topology.Member
import io.grpc.Context

object IdentityContextHelper {
  val storedAuthenticationTokenContextKey: Context.Key[Option[StoredAuthenticationToken]] =
    Context
      .keyWithDefault[Option[StoredAuthenticationToken]]("sequencer-authentication-token", None)

  def getCurrentStoredAuthenticationToken: Option[StoredAuthenticationToken] =
    storedAuthenticationTokenContextKey.get()

  val storedMemberContextKey: Context.Key[Option[Member]] =
    Context.keyWithDefault[Option[Member]]("sequencer-authentication-member", None)

  def getCurrentStoredMember: Option[Member] = storedMemberContextKey.get()
}
