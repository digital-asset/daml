// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.codec.json

import com.digitalasset.transcode.schema.*

import scala.language.implicitConversions

class JsonStringCodec(
    encodeNumericAsString: Boolean = true,
    encodeInt64AsString: Boolean = true,
    removeTrailingNonesInRecords: Boolean = false,
) extends SchemaVisitor.Delegate(
      JsonCodec(
        encodeNumericAsString = encodeNumericAsString,
        encodeInt64AsString = encodeInt64AsString,
        removeTrailingNonesInRecords = removeTrailingNonesInRecords,
      )
    )(
      _.map(_.dimap(ujson.write(_), ujson.read(_)))
    )
