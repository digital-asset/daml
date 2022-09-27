// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8.object_meta

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Assertions._

trait ResourceCreationAnnotationsTests {
  self: ObjectMetaTests with ObjectMetaTestsBase with AnnotationsTests =>

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

}
