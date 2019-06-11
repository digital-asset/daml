package com.digitalasset.ledger.client.services.admin

import com.digitalasset.ledger.api.v1.admin.package_management_service.{ListKnownPackagesRequest, PackageDetails}
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService

import scala.concurrent.{ExecutionContext, Future}

class PackageManagementClient(service: PackageManagementService)(implicit ec: ExecutionContext) {
  def listKnownPackages(): Future[Seq[PackageDetails]] = {
    service
      .listKnownPackages(ListKnownPackagesRequest())
      .map(_.packageDetails)
  }
}
