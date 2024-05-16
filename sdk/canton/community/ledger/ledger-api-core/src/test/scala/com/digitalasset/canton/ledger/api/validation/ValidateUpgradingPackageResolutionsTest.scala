// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.validation.ValidateUpgradingPackageResolutions.ValidatedCommandPackageResolutionsSnapshot
import com.digitalasset.canton.platform.store.packagemeta.{
  PackageMetadataSnapshot,
  PackageMetadataStore,
}
import io.grpc.Status.Code.INVALID_ARGUMENT
import io.grpc.StatusRuntimeException
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class ValidateUpgradingPackageResolutionsTest
    extends AnyWordSpec
    with ValidatorTestUtils
    with TableDrivenPropertyChecks
    with MockitoSugar
    with ArgumentMatchersSugar {

  classOf[ValidateUpgradingPackageResolutionsImpl].getSimpleName should {
    "validate and correctly output package resolution structures" in new TestScope() {
      testResolutions(
        userPreference = Seq(p12, p31),
        expectedPreferenceSetResult = Right {
          // p21 is back-filled
          // p31 is user-preference over the default p32
          Set(p12, p21, p31)
        },
      )
    }

    "use defaults resolutions if user preference is empty" in new TestScope() {
      testResolutions(
        userPreference = Seq.empty,
        expectedPreferenceSetResult = Right(preferenceMapSnapshot.values.toSet),
      )
    }

    "fail on duplicate user preference for same package name" in new TestScope() {
      requestMustFailWith(
        request = validator(Seq(p11, p12)),
        code = INVALID_ARGUMENT,
        description =
          "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: duplicate preference for package-name pkgName1: pkgId11 vs pkgId12",
      )
    }

    "fail on invalid packageId format" in new TestScope {
      requestMustFailWith(
        request = validator(Seq("not%valid^pkgId")),
        code = INVALID_ARGUMENT,
        description =
          """INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: package_id_selection_preference parsing failed with `non expected character 0x25 in Daml-LF Package ID "not%valid^pkgId"`. The package_id_selection_preference field must contain non-empty and valid package ids""",
      )
    }

    "fail on user-specified package-id not found" in new TestScope {
      requestMustFailWith(
        request = validator(Seq("nonExistingPackageId")),
        code = INVALID_ARGUMENT,
        description =
          "INVALID_ARGUMENT(8,0): The submitted request has invalid arguments: user-specified pkg id (nonExistingPackageId) could not be found",
      )
    }
  }

  class TestScope {
    protected val pn1 = Ref.PackageName.assertFromString("pkgName1")
    protected val pn2 = Ref.PackageName.assertFromString("pkgName2")
    protected val pn3 = Ref.PackageName.assertFromString("pkgName3")
    protected val p11 = Ref.PackageId.assertFromString("pkgId11")
    protected val p12 = Ref.PackageId.assertFromString("pkgId12")
    protected val p21 = Ref.PackageId.assertFromString("pkgId21")
    protected val p31 = Ref.PackageId.assertFromString("pkgId31")
    protected val p32 = Ref.PackageId.assertFromString("pkgId32")
    protected val pv1 = Ref.PackageVersion.assertFromString("1")
    protected val pv2 = Ref.PackageVersion.assertFromString("2")

    val preferenceMapSnapshot: Map[Ref.PackageName, Ref.PackageId] =
      Map(pn1 -> p12, pn2 -> p21, pn3 -> p32)
    val packageMapSnapshot: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] =
      Map(
        p11 -> (pn1 -> pv1),
        p12 -> (pn1 -> pv2),
        p21 -> (pn2 -> pv1),
        p31 -> (pn3 -> pv1),
        p32 -> (pn3 -> pv2),
      )

    protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger = NoLogging

    private val packageMetadataSnapshot = mock[PackageMetadataSnapshot]
    when(packageMetadataSnapshot.getUpgradablePackagePreferenceMap).thenReturn(
      preferenceMapSnapshot
    )
    when(packageMetadataSnapshot.getUpgradablePackageMap)
      .thenReturn(packageMapSnapshot)
    private val packageMetadataStore = mock[PackageMetadataStore]
    when(packageMetadataStore.getSnapshot).thenReturn(packageMetadataSnapshot)
    protected val validator = new ValidateUpgradingPackageResolutionsImpl(packageMetadataStore)

    def testResolutions(
        userPreference: Seq[String],
        expectedPreferenceSetResult: Either[StatusRuntimeException, Set[Ref.PackageId]],
    ): Assertion = {
      validator(userPreference) shouldBe expectedPreferenceSetResult.map(
        // If validation is successful, packageMapSnapshot is the same as the one retrieved from the metadata snapshot
        // so we always assert that it is forwarded
        ValidatedCommandPackageResolutionsSnapshot(packageMapSnapshot, _)
      )
    }
  }
}
