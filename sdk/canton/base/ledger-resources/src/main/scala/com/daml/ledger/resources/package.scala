// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import com.daml.resources.{AbstractResourceOwner, Resource as AbstractResource, ResourceFactories}

package object resources {

  type ResourceOwner[+A] = AbstractResourceOwner[ResourceContext, A]

  type Resource[+A] = AbstractResource[ResourceContext, A]

  val Resource = new ResourceFactories[ResourceContext]

}
