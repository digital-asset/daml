// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc

import java.util.concurrent.TimeUnit

import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices, TestConfiguration}
import com.digitalasset.ledger.api.v1.package_service._
import com.google.protobuf.ByteString
import org.scalatest.{FlatSpec, Matchers, OptionValues}

import scala.concurrent.Future

class PackageClientImplTest
    extends FlatSpec
    with Matchers
    with OptionValues
    with DataLayerHelpers
    with PackageClientImplTestHelpers {

  val ledgerServices = new LedgerServices("package-service-ledger")

  behavior of "[7.1] PackageClientImpl.listPackages"

  it should "return the packages from the Ledger" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture("first"),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture) { (client, service) =>
      client
        .listPackages()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst() shouldBe "first"
    }
  }

  behavior of "[7.2] PackageClientImpl.listPackages"

  it should "request the list of packages with the correct ledger ID" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture("first"),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture) { (client, service) =>
      client
        .listPackages()
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingFirst()
      service.getLastListPackageRequest.value.ledgerId shouldBe ledgerServices.ledgerId
    }
  }

  behavior of "[7.3] PackageClientImpl.getPackage"

  it should "return the package from the Ledger" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture) { (client, service) =>
      val getPackage = client
        .getPackage("")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      getPackage.getArchivePayload shouldBe ByteString.EMPTY.toByteArray()
      getPackage.getArchivePayload shouldBe defaultGetPackageResponse.archivePayload.toByteArray
    }
  }

  behavior of "[7.4] PackageClientImpl.getPackage"

  it should "request the package with the correct ledger ID and package ID" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture) { (client, service) =>
      client
        .getPackage("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      service.getLastGetPackagesRequest.value.ledgerId shouldEqual ledgerServices.ledgerId
      service.getLastGetPackagesRequest.value.packageId shouldEqual "packageId"
    }
  }

  behavior of "[7.5] PackageClientImpl.getPackageStatus"

  it should "return the packageStatus from the Ledger" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture) { (client, service) =>
      val getPackageStatus = client
        .getPackageStatus("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      getPackageStatus.getPackageStatusValue.name shouldBe defaultGetPackageStatusResponse.packageStatus.name
    }
  }

  behavior of "[7.6] PackageClientImpl.getPackageStatus"

  it should "send a request with the correct ledger ID and packageID" in {
    ledgerServices.withPackageClient(
      listPackageResponseFuture(),
      defaultGetPackageResponseFuture,
      defaultGetPackageStatusResponseFuture) { (client, service) =>
      client
        .getPackageStatus("packageId")
        .timeout(TestConfiguration.timeoutInSeconds, TimeUnit.SECONDS)
        .blockingGet()
      service.getLastGetPackageStatusRequest.value.ledgerId shouldBe ledgerServices.ledgerId
      service.getLastGetPackageStatusRequest.value.packageId shouldBe "packageId"
    }
  }
}

trait PackageClientImplTestHelpers {
  def listPackageResponse(pids: String*) = new ListPackagesResponse(pids)

  def listPackageResponseFuture(pids: String*) = Future.successful(listPackageResponse(pids: _*))

  val defaultGetPackageResponsePayload = ByteString.EMPTY
  val defaultGetPackageResponse =
    new GetPackageResponse(HashFunction.values.head, defaultGetPackageResponsePayload)
  val defaultGetPackageResponseFuture = Future.successful(defaultGetPackageResponse)
  val defaultGetPackageStatusResponse = new GetPackageStatusResponse(PackageStatus.values.head)
  val defaultGetPackageStatusResponseFuture = Future.successful(defaultGetPackageStatusResponse)
}
