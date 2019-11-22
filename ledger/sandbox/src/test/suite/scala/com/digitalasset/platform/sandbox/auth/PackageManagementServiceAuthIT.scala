// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.nio.file.Files

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.admin.package_management_service._
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.SandboxFixtureWithAuth
import com.google.protobuf.ByteString
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class PackageManagementServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with Matchers
    with Expect {

  private def listKnownPackages(token: Option[String]): Future[ListKnownPackagesResponse] =
    stub(PackageManagementServiceGrpc.stub(channel), token)
      .listKnownPackages(ListKnownPackagesRequest())

  behavior of "PackageManagementService#ListKnownPackages with authorization"

  it should "deny unauthorized calls" in {
    expect(listKnownPackages(None)).toBeDenied
  }
  it should "deny authenticated calls for a user with public claims" in {
    expect(listKnownPackages(Option(rwToken("alice").asHeader()))).toBeDenied
  }
  it should "deny authenticated calls for a user with public read-only claims" in {
    expect(listKnownPackages(Option(roToken("alice").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls for an admin" in {
    expect(listKnownPackages(Option(adminToken.asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens for a user with public claims" in {
    expect(listKnownPackages(Option(rwToken("alice").expired.asHeader()))).toBeDenied
  }
  it should "deny calls with non-expired tokens for a user with public claims" in {
    expect(listKnownPackages(Option(rwToken("alice").expiresTomorrow.asHeader()))).toBeDenied
  }
  it should "deny calls with expired tokens for an admin" in {
    expect(listKnownPackages(Option(adminToken.expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens for an admin" in {
    expect(listKnownPackages(Option(adminToken.expiresTomorrow.asHeader()))).toSucceed
  }

  private val uploadDarFileRequest = new UploadDarFileRequest(
    ByteString.copyFrom(Files.readAllBytes(darFile.toPath)))

  private def uploadDar(token: Option[String]): Future[UploadDarFileResponse] =
    stub(PackageManagementServiceGrpc.stub(channel), token).uploadDarFile(uploadDarFileRequest)

  behavior of "PackageManagementService#UploadDAR with authorization"

  it should "deny unauthorized calls" in {
    expect(uploadDar(None)).toBeDenied
  }
  it should "deny authenticated calls for a user with public claims" in {
    expect(uploadDar(Option(rwToken("alice").asHeader()))).toBeDenied
  }
  it should "deny authenticated calls for a user with public read-only claims" in {
    expect(uploadDar(Option(roToken("alice").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls for an admin" in {
    expect(uploadDar(Option(adminToken.asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens for a user with public claims" in {
    expect(uploadDar(Option(rwToken("alice").expired.asHeader()))).toBeDenied
  }
  it should "deny calls with non-expired tokens for a user with public claims" in {
    expect(uploadDar(Option(rwToken("alice").expiresTomorrow.asHeader()))).toBeDenied
  }
  it should "deny calls with expired tokens for an admin" in {
    expect(uploadDar(Option(adminToken.expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens for an admin" in {
    expect(uploadDar(Option(adminToken.expiresTomorrow.asHeader()))).toSucceed
  }

}
