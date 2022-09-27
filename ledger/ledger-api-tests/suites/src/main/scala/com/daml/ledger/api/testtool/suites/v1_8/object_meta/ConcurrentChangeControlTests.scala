// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import com.daml.ledger.api.testtool.infrastructure.Assertions._

import scala.concurrent.Future

trait ConcurrentChangeControlTests { self: ObjectMetaTests with ObjectMetaTestsBase =>

  testWithFreshResource(
    "UpdatingResourceUtilizingConcurrentChangeControl",
    "Updating a resource utilizing the concurrent change control by means of the resource version",
  )()(implicit ec =>
    implicit ledger =>
      resource => {
        val rv1 = extractMetadata(resource).resourceVersion
        for {
          // Updating a resource with the concurrent change detection disabled
          rv2: String <- update(
            id = getId(resource),
            annotations = Map(
              "k1" -> "v1",
              "k2" -> "v2",
            ),
            resourceVersion = "",
            updatePaths = Seq(annotationsUpdatePath),
          ).map { metadata =>
            assertEquals(
              metadata.annotations,
              Map("k1" -> "v1", "k2" -> "v2"),
            )
            assertValidResourceVersionString(rv1, "a new resource")
            val rv2 = metadata.resourceVersion
            assertValidResourceVersionString(rv2, "an updated resource")
            assert(
              rv1 != rv2,
              s"A resource's resource_version before and after an update must be different but was the same: '$rv2'",
            )
            rv2
          }
          // Updating a resource with the concurrent change detection enabled but providing an outdated resource version.
          _ <- update(
            id = getId(resource),
            annotations = Map(
              "k1" -> "v1",
              "k2" -> "v2",
            ),
            resourceVersion = rv1,
            updatePaths = Seq(annotationsUpdatePath),
          ).mustFailWith(
            "updating a resource using an outdated resource version",
            concurrentUserUpdateDetectedErrorDescription(
              id = getId(resource)
            ),
          )
          // Updating a resource with the concurrent change detection enabled and prlviding the up-to-date resource version
          _ <- update(
            id = getId(resource),
            annotations = Map(
              "k1" -> "v1a",
              "k2" -> "",
              "k3" -> "v3",
            ),
            resourceVersion = rv2,
            updatePaths = Seq(annotationsUpdatePath),
          ).map { metadata =>
            assertEquals(
              metadata.annotations,
              Map("k1" -> "v1a", "k3" -> "v3"),
            )
            val rv3 = metadata.resourceVersion
            assert(
              rv2 != rv3,
              s"A resource's resource_version before and after an update must be different but was the same: '$rv2'",
            )
            assertValidResourceVersionString(
              rv3,
              "updating a resource using the up-to-date resource version",
            )
          }
        } yield ()
      }
  )

  testWithFreshResource(
    "RaceConditionUpdateResourceAnnotations",
    "Tests scenario of multiple concurrent update annotations RPCs for the same resource",
  )()(implicit ec =>
    implicit ledger =>
      resource => {
        val attempts = (1 to 10).toVector
        for {
          _ <- Future.traverse(attempts) { attemptNo =>
            update(
              id = getId(resource),
              annotations = Map(s"key$attemptNo" -> "a"),
              updatePaths = Seq(annotationsUpdatePath),
            )
          }
          annotations <- fetchNewestAnnotations(id = getId(resource))
        } yield {
          assertEquals(
            annotations,
            Map(
              "key1" -> "a",
              "key2" -> "a",
              "key3" -> "a",
              "key4" -> "a",
              "key5" -> "a",
              "key6" -> "a",
              "key7" -> "a",
              "key8" -> "a",
              "key9" -> "a",
              "key10" -> "a",
            ),
          )
        }
      }
  )

}
