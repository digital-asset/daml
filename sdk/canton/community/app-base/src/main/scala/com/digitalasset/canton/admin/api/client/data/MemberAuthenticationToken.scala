// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.Member
import com.google.protobuf.ByteString

import java.util.Base64

/** Token that can be used to authenticate a member on the sequencer API
  * @param member
  *   member the token is for
  * @param token
  *   token
  * @param expiresAt
  *   timestamp at which the token expires
  */
final case class MemberAuthenticationToken(
    member: Member,
    token: ByteString,
    expiresAt: CantonTimestamp,
) {

  /** Authentication token base64 encoded
    */
  def tokenAsBase64: String = Base64.getEncoder.encodeToString(token.toByteArray)
}
