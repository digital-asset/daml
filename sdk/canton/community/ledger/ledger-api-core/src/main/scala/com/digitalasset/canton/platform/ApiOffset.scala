// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.data.Offset

// This utility object is used as a single point to encode and decode
// offsets sent over the API and received from the API.
object ApiOffset {

  private def assertFromString(s: String): Option[Offset] =
    Offset.tryFromString(s).fold(throw _, identity)

  // TODO(#18685) remove converter as it should be unused
  def assertFromStringToLongO(s: String): Option[Long] =
    assertFromString(s).map(_.unwrap)

  // TODO(#18685) remove converter as it should be unused
  def assertFromStringToLong(s: String): Long =
    assertFromStringToLongO(s).getOrElse(0L)

  // TODO(#18685) remove converter as it should be unused
  def fromLong(l: Long): String =
    Offset.tryFromLong(l).toHexString
}
