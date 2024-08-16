// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.transcode.daml_lf

import com.digitalasset.daml.lf.data.Ref

/** Top-level Ledger Schema entity. This can either be a template/interface or a choice definition. */
sealed trait SchemaEntity[P] { def map[Q](f: P => Q): SchemaEntity[Q] }
object SchemaEntity {
  final case class PackageInfo(
      name: Ref.PackageName,
      version: Ref.PackageVersion,
  )

  final case class Template[P](
      id: Ref.Identifier,
      packageInfo: PackageInfo,
      payload: P,
      key: Option[P],
      kind: Template.DataKind,
      implements: Seq[Ref.Identifier],
  ) extends SchemaEntity[P] {
    def map[Q](f: P => Q): SchemaEntity[Q] = copy(payload = f(payload), key = key.map(f))
  }

  object Template {
    sealed trait DataKind
    object DataKind {
      case object Template extends DataKind
      case object Interface extends DataKind
    }
  }

  final case class Choice[P](
      entityId: Ref.Identifier,
      packageInfo: PackageInfo,
      choiceName: Ref.ChoiceName,
      argument: P,
      result: P,
      consuming: Boolean,
  ) extends SchemaEntity[P] {
    def map[Q](f: P => Q): SchemaEntity[Q] = copy(argument = f(argument), result = f(result))
  }

  /** Returns sequence of [[SchemaEntity]] instances for further processing. */
  def collect[A]: CollectResult[A, Seq[SchemaEntity[A]]] = identity

  def map[A, B](f: A => B): CollectResult[A, Seq[SchemaEntity[B]]] = entities =>
    entities.map(_.map(f))
}
