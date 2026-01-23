// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

final case class Choice[Type](
    name: ChoiceName,
    consuming: Boolean,
    argument: Type,
    result: Type,
):
  def map[U](f: Type => U): Choice[U] =
    copy(argument = f(argument), result = f(result))

  def zipWith[U, V](other: Choice[U])(combine: (Type, U) => V): Choice[V] =
    copy(argument = combine(argument, other.argument), result = combine(result, other.result))
