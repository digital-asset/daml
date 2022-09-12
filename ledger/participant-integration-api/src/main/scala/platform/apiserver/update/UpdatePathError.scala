// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.update

sealed trait UpdatePathError {
  def getReason: String = getClass.getSimpleName
}

object UpdatePathError {

  final case class MergeUpdateModifierOnEmptyMapField(rawPath: String) extends UpdatePathError

  final case class MergeUpdateModifierOnPrimitiveFieldDefaultValueUpdate(rawPath: String)
      extends UpdatePathError

  final case class UnknownFieldPath(rawPath: String) extends UpdatePathError

  final case class UnknownUpdateModifier(rawPath: String) extends UpdatePathError

  final case class InvalidUpdatePathSyntax(rawPath: String) extends UpdatePathError

  final case class EmptyFieldPath(rawPath: String) extends UpdatePathError

  final case class DuplicatedFieldPath(rawPath: String) extends UpdatePathError

  final case object EmptyFieldMask extends UpdatePathError
}
