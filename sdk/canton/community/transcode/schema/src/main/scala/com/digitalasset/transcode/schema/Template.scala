// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.schema

final case class Template[Type](
    templateId: Identifier,
    payload: Type,
    key: Option[Type],
    isInterface: Boolean,
    implements: Seq[Identifier],
    choices: Seq[Choice[Type]],
):
  def map[U](f: Type => U): Template[U] =
    copy(payload = f(payload), key = key.map(f), choices = choices.map(_.map(f)))

  def zipWith[U, V](other: Template[U])(combine: (Type, U) => V): Template[V] =
    copy(
      payload = combine(payload, other.payload),
      key = key.zip(other.key).map(combine.tupled),
      choices = {
        val thisMap = choices.map(c => c.name -> c).toMap
        val thatMap = other.choices.map(c => c.name -> c).toMap
        val names = thisMap.keySet intersect thatMap.keySet
        names.toSeq.map(name => thisMap(name).zipWith(thatMap(name))(combine))
      },
    )
