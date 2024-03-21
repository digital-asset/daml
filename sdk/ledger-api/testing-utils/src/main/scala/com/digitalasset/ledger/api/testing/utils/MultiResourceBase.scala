// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testing.utils

import org.scalatest.AsyncTestSuite

import scala.collection.immutable

trait MultiResourceBase[FixtureId, TestContext]
    extends MultiFixtureBase[FixtureId, TestContext]
    with SuiteResource[Map[FixtureId, () => TestContext]] { self: AsyncTestSuite =>

  /** Overriding this provides an easy way to narrow down testing to a single implementation. */
  protected def fixtureIdsEnabled: Set[FixtureId]

  protected def constructResource(index: Int, fixtureId: FixtureId): Resource[TestContext]

  override protected lazy val suiteResource: Resource[Map[FixtureId, () => TestContext]] = {
    MultiResource(fixtureIdsEnabled.zipWithIndex.map { case (backend, idx) =>
      backend -> constructResource(idx, backend)
    }.toMap)
  }

  override protected lazy val fixtures: immutable.Iterable[TestFixture] =
    fixtureIdsEnabled.map { implementation =>
      TestFixture(implementation, suiteResource.value(implementation))
    }
}

case class MultiResource[FixtureId, TestContext](resources: Map[FixtureId, Resource[TestContext]])
    extends Resource[Map[FixtureId, () => TestContext]] {
  override lazy val value: Map[FixtureId, () => TestContext] =
    resources.view.mapValues(r => () => r.value).toMap
  override def setup(): Unit = resources.foreach(_._2.setup())
  override def close(): Unit = resources.foreach(_._2.close())
}
