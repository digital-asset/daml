// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.objectmeta

import java.nio.charset.StandardCharsets

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Assertions._

import scala.concurrent.Future

// Gather all the tests common to resources that support ObjectMeta metadata
trait ObjectMetaTests extends ObjectMetaTestsBase {

  private[objectmeta] def maxAnnotationsSizeInBytes = 256 * 1024
  private[objectmeta] def valueExceedingAnnotationsLimit = "a" * maxAnnotationsSizeInBytes
  private[objectmeta] def largestAllowedValue = "a" * (maxAnnotationsSizeInBytes - 1)
  private[objectmeta] def annotationsOverSizeLimit = Map("a" -> largestAllowedValue, "c" -> "d")
  private[objectmeta] def annotationsBelowMaxSizeLimitBecauseNotCountingEmptyValuedKeys =
    Map("a" -> largestAllowedValue, "cc" -> "")

  private[objectmeta] def getAnnotationsBytes(annotations: Map[String, String]): Int =
    annotations.iterator.map { case (k, v) =>
      k.getBytes(StandardCharsets.UTF_8).length + v.getBytes(StandardCharsets.UTF_8).length
    }.sum

  assertEquals(
    valueExceedingAnnotationsLimit.getBytes(StandardCharsets.UTF_8).length,
    maxAnnotationsSizeInBytes,
  )
  assertEquals(
    getAnnotationsBytes(annotationsOverSizeLimit),
    getAnnotationsBytes(annotationsBelowMaxSizeLimitBecauseNotCountingEmptyValuedKeys),
  )

  private[objectmeta] def invalidKey = ".aaaa.management.daml/foo_"
  private[objectmeta] def validKey = "0-aaaa.management.daml/foo"
  testWithFreshResource(
    "AllowSpecifyingResourceVersionAndResourceIdInUpdateMask",
    "Allow specifying resource_version and resource's id fields in the update mask",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map.empty,
          resourceVersion = "",
          updatePaths = Seq(resourceIdPath, resourceVersionUpdatePath),
        ).map { objectMeta =>
          assertEquals(
            objectMeta.annotations,
            extractAnnotations(resource),
          )
        }
  )

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
            concurrentUserUpdateDetectedErrorCode,
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

  testWithFreshResource(
    "FailingUpdateRequestsWhenUpdatePathIsDuplicated",
    "Failing an update request when an update path is duplicated",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map("k1" -> "v1"),
          updatePaths = Seq(annotationsUpdatePath, annotationsUpdatePath),
        ).mustFailWith(
          "updating a resource",
          invalidUpdateRequestErrorCode,
        )
  )

  testWithFreshResource(
    "FailingUpdateRequestWhenNoUpdatePaths",
    "Failing an update request when the update mask is empty",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map("k1" -> "v1"),
          updatePaths = Seq.empty,
        )
          .mustFailWith(
            "updating a resource",
            invalidUpdateRequestErrorCode,
          )
  )

  testWithFreshResource(
    "FAilingUpdateRequestWhenUpdateMaskHaveUnknownFieldPath",
    "Failing an update request when the update mask contains a path to an unknown field",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        for {
          _ <- update(
            id = getId(resource),
            annotations = Map.empty,
            updatePaths = Seq("unknown_field"),
          ).mustFailWith(
            "fail 1",
            invalidUpdateRequestErrorCode,
          )
          _ <- update(
            id = getId(resource),
            annotations = Map.empty,
            updatePaths = Seq("aaa!bbb"),
          ).mustFailWith(
            "fail 2",
            invalidUpdateRequestErrorCode,
          )
          _ <- update(
            id = getId(resource),
            annotations = Map.empty,
            updatePaths = Seq(""),
          ).mustFailWith(
            "fail 3",
            invalidUpdateRequestErrorCode,
          )
        } yield ()
  )

  testWithoutResource(
    "FailingToCreateResourceWhenAnnotationsValueIsEmpty",
    "Failing to create a resource when an annotations' value is empty",
  )(implicit ec => { implicit ledger =>
    createResourceWithAnnotations(
      annotations = Map("k2" -> "")
    ).mustFailWith(
      "creating a resource",
      LedgerApiErrors.RequestValidation.InvalidArgument,
      Some(
        "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: The value of an annotation is empty for key: 'k2'"
      ),
    )
  })

  testWithoutResource(
    "TestAnnotationsKeySyntaxOnResourceCreation",
    "Test the annotations' key syntax on a resource creation",
  )(implicit ec => { implicit ledger =>
    createResourceWithAnnotations(
      annotations = Map(invalidKey -> "a")
    ).mustFailWith(
      "creating a resource",
      errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
      exceptionMessageSubstring = Some(
        "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Key prefix segment '.aaaa.management.daml' has invalid syntax"
      ),
    )
  })

  testWithoutResource(
    "TestAnnotationsSizeLimitsOnResourceCreation",
    "Test annotations' size limit on creation",
  ) { implicit ec => implicit ledger =>
    createResourceWithAnnotations(annotations = Map("a" -> valueExceedingAnnotationsLimit))
      .mustFailWith(
        "total size of annotations exceeds 256kb max limit",
        errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: annotations from field '${annotationsUpdateRequestFieldPath}' are larger than the limit of 256kb"
        ),
      )
  }

  testWithFreshResource(
    "UpdateAnnotationsUsingNonEmptyMap",
    "Update the annotations using an update paths with a non-empty map value",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))(implicit ec =>
    implicit ledger =>
      resource => {

        def updateAnnotations(updatePath: String): Future[Unit] = {
          update(
            id = getId(resource),
            annotations = Map(
              // update a value for an existing key
              "k1" -> "v1a",
              // remove an existing key-value pair
              "k3" -> "",
              // add a new key-value pair
              "k4" -> "v4",
              // attempt to remove a key-value pair which doesn't exist
              "k5" -> "",
            ),
            updatePaths = Seq(updatePath),
          ).map { objectMeta =>
            assertEquals(
              "updating annotations",
              objectMeta.annotations,
              Map("k1" -> "v1a", "k2" -> "v2", "k4" -> "v4"),
            )
          }
        }

        for {
          _ <- updateAnnotations(annotationsUpdatePath)
          _ <- updateAnnotations(annotationsShortUpdatePath)
        } yield ()
      }
  )

  testWithFreshResource(
    "UpdateAnnotationsUsingEmptyMap",
    "Update the annotations using update paths with the empty map value",
  )(annotations = Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))(implicit ec =>
    implicit ledger =>
      resource => {

        def updateAnnotations(updatePath: String): Future[Unit] = {
          update(
            id = getId(resource),
            annotations = Map.empty,
            updatePaths = Seq(updatePath),
          ).map { objectMeta =>
            assertEquals(
              "updating the annotations",
              objectMeta.annotations,
              Map("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"),
            )
          }
        }

        for {
          _ <- updateAnnotations(annotationsUpdatePath)
          _ <- updateAnnotations(annotationsShortUpdatePath)
        } yield ()
      }
  )

  testWithFreshResource(
    "TestAnnotationsKeySyntaxOnResourceUpdateWhenAddingKey",
    "Test the annotations' key syntax for the update RPC",
  )(annotations = Map(validKey -> "a"))(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map(invalidKey -> "a"),
          updatePaths = Seq(annotationsUpdatePath),
        ).mustFailWith(
          "updating the annotations",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Key prefix segment '.aaaa.management.daml' has invalid syntax"
          ),
        )
  )

  testWithFreshResource(
    "TestAnnotationsKeySyntaxOnResourceUpdateEvenWhenDeletingNonExistentKey",
    "Test the annotations' key syntax for the update RPC even for map entries that represent a deletion of a non-existent key",
  )()(implicit ec =>
    implicit ledger =>
      resource =>
        update(
          id = getId(resource),
          annotations = Map(invalidKey -> ""),
          updatePaths = Seq(annotationsUpdatePath),
        ).mustFailWith(
          "deleting an annotations' key",
          errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
          exceptionMessageSubstring = Some(
            "INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: Key prefix segment '.aaaa.management.daml' has invalid syntax"
          ),
        )
  )

  testWithFreshResource(
    "NotCountingEmptyValuedKeysToTheAnnotationsSizeLimitOnUpdate",
    "Do not count the keys in the provided annotations map that correspond to deletions, towards the annotations' nax size limit",
  )() { implicit ec => implicit ledger => resource =>
    update(
      id = getId(resource),
      annotations = annotationsBelowMaxSizeLimitBecauseNotCountingEmptyValuedKeys,
      updatePaths = Seq(annotationsUpdatePath),
    ).map { objectMeta =>
      assertEquals(
        "updating and not exceeding annotations' max size limit because deletions are not counted towards the limit",
        objectMeta.annotations,
        Map("a" -> largestAllowedValue),
      )
    }
  }

  testWithFreshResource(
    "TestAnnotationsMaxSizeLimitsWhenUpdatingResource",
    "Test the annotations' max size limit on a resource update RPC",
  )(annotations = Map("a" -> largestAllowedValue)) { implicit ec => implicit ledger => resource =>
    update(
      id = getId(resource),
      annotations = Map("a" -> valueExceedingAnnotationsLimit),
      updatePaths = Seq(annotationsUpdatePath),
    )
      .mustFailWith(
        "total size of annotations, in a user update call, is over 256kb",
        errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = Some(
          s"INVALID_ARGUMENT: INVALID_ARGUMENT(8,0): The submitted command has invalid arguments: annotations from field '${annotationsUpdateRequestFieldPath}' are larger than the limit of 256kb"
        ),
      )
  }

}
