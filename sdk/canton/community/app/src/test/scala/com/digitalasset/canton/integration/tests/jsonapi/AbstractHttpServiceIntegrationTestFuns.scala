// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.testing.utils.TestModels.{
  com_daml_ledger_test_ModelTestDar_path,
  com_daml_ledger_test_SemanticTestDar_path,
}
import com.digitalasset.canton.util.JarResourceUtils
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.http.scaladsl.model.*

object AbstractHttpServiceIntegrationTestFuns {
  val dar1 = JarResourceUtils.resourceFile(com_daml_ledger_test_ModelTestDar_path)

  val dar2 = JarResourceUtils.resourceFile("Account-3.4.0.dar")

  val dar3 = JarResourceUtils.resourceFile(com_daml_ledger_test_SemanticTestDar_path)

  val userDar = JarResourceUtils.resourceFile("User-3.4.0.dar")

  val ciouDar = JarResourceUtils.resourceFile("CIou-3.4.0.dar")

  val fooV1Dar = JarResourceUtils.resourceFile("foo-0.0.1.dar")
  val fooV2Dar = JarResourceUtils.resourceFile("foo-0.0.2.dar")

  private[this] def packageIdOfDar(darFile: java.io.File): Ref.PackageId = {
    import com.digitalasset.daml.lf.{archive, typesig}
    val dar = archive.UniversalArchiveReader.assertReadFile(darFile)
    val pkgId = typesig.PackageSignature.read(dar.main)._2.packageId
    Ref.PackageId.assertFromString(pkgId)
  }

  lazy val pkgIdCiou = packageIdOfDar(ciouDar)
  lazy val pkgIdModelTests = packageIdOfDar(dar1)
  lazy val pkgIdUser = packageIdOfDar(userDar)
  lazy val pkgIdFooV1 = packageIdOfDar(fooV1Dar)
  lazy val pkgIdFooV2 = packageIdOfDar(fooV2Dar)
  lazy val pkgIdAccount = packageIdOfDar(dar2)

  lazy val pkgNameCiou = Ref.PackageName.assertFromString("CIou")
  lazy val pkgNameModelTests = Ref.PackageName.assertFromString("model-tests")
  lazy val pkgNameAccount = Ref.PackageName.assertFromString("Account")

  trait UriFixture {
    def uri: Uri
  }
  final case class HttpServiceOnlyTestFixtureData(
      uri: Uri
  ) extends UriFixture

  final case class HttpServiceTestFixtureData(
      uri: Uri,
      client: DamlLedgerClient,
  ) extends UriFixture
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait AbstractHttpServiceIntegrationTestFuns extends HttpJsonApiTestBase with HttpTestFuns {
  import AbstractHttpServiceIntegrationTestFuns.*

  override def packageFiles = List(dar1, dar2, userDar)

}
