// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.dependencygraph

final case class Node[+K, +A](content: A, dependencies: List[K], collectDepError: Boolean)
