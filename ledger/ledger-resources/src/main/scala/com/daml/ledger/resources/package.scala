// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import com.daml.resources.AbstractResourceOwner

import scala.concurrent.ExecutionContext

package object resources {

  type ResourceOwner[+A] = AbstractResourceOwner[ExecutionContext, A]

}
