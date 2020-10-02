// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.dependencygraph

sealed abstract class BaseNode[+K, +A] {
  def dependencies: List[K]
}

final case object UnknownPlaceholder extends BaseNode[Nothing, Nothing] {
  override def dependencies = List.empty
}

final case class Node[+K, +A](content: A, dependencies: List[K], collectDepError: Boolean)
    extends BaseNode[K, A]

final case class CannotBuildGraphException(msg: String) extends RuntimeException(msg)
