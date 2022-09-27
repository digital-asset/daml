// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Assertions._

import scala.concurrent.Future

trait ResourceUpdateAnnotationsTests {
  self: ObjectMetaTests with ObjectMetaTestsBase with AnnotationsTests =>

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
    "TestAnnotationsKeySyntaxOnResourceUpdate",
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
