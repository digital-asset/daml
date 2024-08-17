// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

import com.digitalasset.daml.lf.data.Ref

/** Dictionary of templates, keys, choice arguments and choice results. */
final case class Dictionary[T](
    templates: Map[Ref.Identifier, T],
    templateKeys: Map[Ref.Identifier, T],
    choiceArguments: Map[(Ref.Identifier, Ref.ChoiceName), T],
    choiceResults: Map[(Ref.Identifier, Ref.ChoiceName), T],
)

object Dictionary {

  /** Collect [[SchemaEntity]] instances into a [[com.digitalasset.daml.lf.data.Ref.Identifier]]-keyed dictionary. Usable in codecs.
    */
  def collect[T]: CollectResult[T, Dictionary[T]] = (entities: Seq[SchemaEntity[T]]) =>
    Dictionary(
      entities.collect { case SchemaEntity.Template(id, pkgInfo, payload, key, kind, implements) =>
        id -> payload
      }.toMap,
      entities.collect {
        case SchemaEntity.Template(id, pkgInfo, payload, Some(key), kind, implements) => id -> key
      }.toMap,
      entities.collect {
        case SchemaEntity.Choice(entityId, pkgInfo, choiceName, argument, result, consuming) =>
          (entityId, choiceName) -> argument
      }.toMap,
      entities.collect {
        case SchemaEntity.Choice(entityId, pkgInfo, choiceName, argument, result, consuming) =>
          (entityId, choiceName) -> result
      }.toMap,
    )

}
