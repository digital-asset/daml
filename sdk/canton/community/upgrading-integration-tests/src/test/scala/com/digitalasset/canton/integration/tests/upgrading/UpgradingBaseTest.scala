// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.commands
import com.daml.ledger.javaapi
import com.digitalasset.canton.BaseTest.getResourcePath
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPackageId, LfPackageName}
import monocle.Monocle.toAppliedFocusOps
import org.scalactic.source
import org.scalatest.verbs.ShouldVerb
import org.scalatest.wordspec.FixtureAnyWordSpec

object UpgradingBaseTest extends ShouldVerb {
  lazy val UpgradeV1: String = getResourcePath("Upgrade-1.0.0.dar")
  lazy val UpgradeV2: String = getResourcePath("Upgrade-2.0.0.dar")
  lazy val AppUpgradeV1: String = getResourcePath("AppUpgrade-1.0.0.dar")
  lazy val AppUpgradeV2: String = getResourcePath("AppUpgrade-2.0.0.dar")

  lazy val NonConformingV1: String = getResourcePath("NonConforming-1.0.0.dar")
  lazy val NonConformingV2: String = getResourcePath("NonConforming-2.0.0.dar")
  lazy val NonConformingX: String = getResourcePath("NonConformingX-1.0.0.dar")

  lazy val AppInstallV1: String = getResourcePath("tests-app-install-1.0.0.dar")
  lazy val AppInstallV2: String = getResourcePath("tests-app-install-2.0.0.dar")
  lazy val FeaturedAppRightIface: String = getResourcePath(
    "tests-featured-app-right-iface-1.0.0.dar"
  )
  lazy val FeaturedAppRightImplV1: String = getResourcePath(
    "tests-featured-app-right-impl-1.0.0.dar"
  )
  lazy val FeaturedAppRightImplV2: String = getResourcePath(
    "tests-featured-app-right-impl-2.0.0.dar"
  )

  lazy val HoldingV1: String = getResourcePath("tests-Holding-v1-1.0.0.dar")
  lazy val HoldingV2: String = getResourcePath("tests-Holding-v2-1.0.0.dar")
  lazy val TokenV1: String = getResourcePath("tests-Token-1.0.0.dar")
  lazy val TokenV2: String = getResourcePath("tests-Token-2.0.0.dar")
  lazy val TokenV3: String = getResourcePath("tests-Token-3.0.0.dar")
  lazy val TokenV4: String = getResourcePath("tests-Token-4.0.0.dar")

  lazy val DvpAssetsV1: String = getResourcePath("dvp-assets-1.0.0.dar")
  lazy val DvpAssetsV2: String = getResourcePath("dvp-assets-2.0.0.dar")

  lazy val DvpAssetFactoryV1: String = getResourcePath("dvp-asset-factory-1.0.0.dar")
  lazy val DvpAssetFactoryV2: String = getResourcePath("dvp-asset-factory-2.0.0.dar")

  lazy val DvpOffersV1: String = getResourcePath("dvp-offer-1.0.0.dar")
  lazy val DvpOffersV2: String = getResourcePath("dvp-offer-2.0.0.dar")

  lazy val IBaz: String = getResourcePath("ibaz-1.0.0.dar")
  lazy val IBar: String = getResourcePath("ibar-1.0.0.dar")
  lazy val BarV1: String = getResourcePath("bar-1.0.0.dar")
  lazy val BarV2: String = getResourcePath("bar-2.0.0.dar")
  lazy val BazV1: String = getResourcePath("baz-1.0.0.dar")
  lazy val BazV2: String = getResourcePath("baz-2.0.0.dar")
  lazy val FooV1: String = getResourcePath("foo-1.0.0.dar")
  lazy val FooV2: String = getResourcePath("foo-2.0.0.dar")
  lazy val FooV3: String = getResourcePath("foo-3.0.0.dar")
  lazy val FooV4: String = getResourcePath("foo-4.0.0.dar")
  lazy val UtilV1: String = getResourcePath("util-1.0.0.dar")
  lazy val UtilV2: String = getResourcePath("util-2.0.0.dar")

  /** The DARs above are used to test interface fetches that, at time of writing, is a 2.dev
    * feature. For this reason only tests running a dev version of canton can use them.
    *
    * Once TransactionVersion.minFetchInterfaceId is a final LF version this can be removed
    */
  def testedPV(testedProtocolVersion: ProtocolVersion): Boolean =
    testedProtocolVersion >= ProtocolVersion.dev

  trait WhenPV {
    self: FixtureAnyWordSpec =>
    protected val testedProtocolVersion: ProtocolVersion
    implicit class Wrapper(name: String) {
      def whenUpgradeTestPV(f: => Unit)(implicit pos: source.Position): Unit =
        if (testedPV(testedProtocolVersion)) {
          name when f
        } else {
          ()
        }
    }
  }

  implicit class CommandsWithExplicitPackageId(commandJava: javaapi.data.Command) {
    def withPackageId(packageId: String): javaapi.data.Command = {
      val command = commands.Command.fromJavaProto(commandJava.toProtoCommand)
      val res = command.command match {
        case commands.Command.Command.Empty => command
        case c: commands.Command.Command.Create =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageId)))
          )
        case c: commands.Command.Command.Exercise =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageId)))
          )
        case c: commands.Command.Command.ExerciseByKey =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageId)))
          )
        case c: commands.Command.Command.CreateAndExercise =>
          command.copy(command =
            c.focus(_.value.templateId).modify(_.map(_.copy(packageId = packageId)))
          )
      }
      javaapi.data.Command.fromProtoCommand(commands.Command.toJavaProto(res))
    }
  }

  object Syntax {
    implicit class LfPackageTypeConversions(pkgRefStr: String) {
      def toPackageId: LfPackageId = LfPackageId.assertFromString(pkgRefStr)
      def toPackageName: LfPackageName = LfPackageName.assertFromString(pkgRefStr)
    }

    implicit class PackageIdVettingExtensions(packageId: LfPackageId) {
      def withNoVettingBounds: VettedPackage = VettedPackage(
        packageId = packageId,
        validFromInclusive = None,
        validUntilExclusive = None,
      )

      def withVettingEndsAt(validUntil: CantonTimestamp): VettedPackage = VettedPackage(
        packageId = packageId,
        validFromInclusive = None,
        validUntilExclusive = Some(validUntil),
      )
    }
  }
}
