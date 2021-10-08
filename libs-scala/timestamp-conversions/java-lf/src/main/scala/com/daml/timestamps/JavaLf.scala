// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.lf.data.Time.{Timestamp => LfTimestamp}

import java.time.{Instant => JavaTimestamp}

object AsLf {
  implicit final class ToLfTimestampConversions[T](timestamp: T)(implicit cv: T TConv LfTimestamp) {
    def asLf: LfTimestamp = cv(timestamp)
  }

  // for example, you can put both of these instances in `Timestamp`'s companion
  // and you don't have to import them then.  The kind of granularity you're
  // imagining is going to make that a good idea only when it's reasonable to
  // _always_ expect the other to be there; that is true for these examples, as
  // it is unreasonable to have LfTimestamp available but not JavaTimestamp.
  // For other cases, you'll want an explicit import.
  implicit val `java to lf timestamp`: JavaTimestamp TConv LfTimestamp =
    LfTimestamp.assertFromInstant // explained the issue with this in the original PR
  implicit val `lf to java timestamp`: LfTimestamp TConv JavaTimestamp = _.toInstant
}
