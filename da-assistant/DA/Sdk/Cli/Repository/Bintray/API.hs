-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE FlexibleInstances #-}

module DA.Sdk.Cli.Repository.Bintray.API
    ( BintrayManagementAPIWithAuth
    , BintrayManagementAPINoAuth
    , BintrayDownloadAPIWithAuth
    , BintrayDownloadAPINoAuth
    ) where

import qualified DA.Sdk.Cli.Repository.Types       as Ty
import           Data.ByteString                   (ByteString)
import           Data.Text                         (Text)
import           Servant.API

-- definitions:
--   package: a package with a subject, a repo and a package name
--   release: the release of a package which includes package and a version

-- Get Package Attributes
-- GET /packages/:subject/:repo/:package/attributes
type GetPackageAttributesAPI =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> "attributes"
        :> Get '[JSON] [Ty.Attribute]

-- Get Attributes
-- GET /packages/:subject/:repo/:package/versions/:version/attributes
type GetReleaseAttributesAPI =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> "versions"
        :> Capture "version" Ty.GenericVersion
        :> "attributes"
        :> Get '[JSON] [Ty.Attribute]

-- Update Attributes
-- PATCH /packages/:subject/:repo/:package/versions/:version/attributes
type UpdateReleaseAttributesAPI =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> "versions"
        :> Capture "version" Ty.GenericVersion
        :> "attributes"
        :> ReqBody '[JSON] [Ty.Attribute]
        :> Patch '[JSON] [Ty.Attribute]

-- Update Attributes
-- PATCH /package/:subject/:repo/:package/attributes
type UpdatePackageAttributesAPI =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> "attributes"
        :> ReqBody '[JSON] [Ty.Attribute]
        :> Patch '[JSON] NoContent

-- Attribute Search
-- POST /search/attributes/:subject/:repo[?attribute_values=1]
type AttributeSearchAPI =
    "search"
        :> "attributes"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> ReqBody '[JSON] [Ty.SearchAttribute]
        :> QueryParam "attribute_values" Ty.BitFlag
        :> Post '[JSON] [Ty.PackageInfo]


-- Get Package
-- GET /packages/:subject/:repo/:package[?attribute_values=1]
type GetPackageAPI =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> QueryParam "attribute_values" Ty.BitFlag
        :> Get '[JSON] Ty.BintrayPackage

-- Create Package
-- POST /packages/:subject/:repo
type CreatePackageAPI =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> ReqBody '[JSON] Ty.PackageDescriptor
        :> Post '[JSON] NoContent


-- Upload Content
-- PUT /content/:subject/:repo/:package/:version/:file_path[?publish=0/1][?override=0/1][?explode=0/1]
type UploadReleaseAPI =
    "content"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> Capture "version" Ty.GenericVersion
        :> CaptureAll "filepath" Text
        :> QueryParam "publish" Ty.BitFlag
        :> ReqBody '[OctetStream] ByteString
        :> Put '[JSON] NoContent

-- Get Repositories
-- GET /repos/:subject
type GetRepositoriesAPI =
    "repos"
        :> Capture "subject" Ty.Subject
        :> Get '[JSON] [Ty.Repository]

-- Get Version Files
-- GET /packages/:subject/:repo/:package/versions/:version/files[?include_unpublished=0/1]
type GetVersionFiles =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> "versions"
        :> Capture "version" Ty.GenericVersion
        :> "files"
        :> QueryParam "include_unpublished" Ty.BitFlag
        :> Get '[JSON] [Ty.PkgFileInfo]

-- Get Version
-- GET /packages/:subject/:repo/:package/versions/_latest
type GetLatestVersionAPI =
    "packages"
        :> Capture "subject" Ty.Subject
        :> Capture "repository" Ty.Repository
        :> Capture "package" Ty.PackageName
        :> "versions"
        :> "_latest"
        :> Get '[JSON] Ty.LatestVersion

type BintrayManagementAPINoAuth =
         GetReleaseAttributesAPI
    :<|> GetPackageAttributesAPI
    :<|> UpdateReleaseAttributesAPI
    :<|> UpdatePackageAttributesAPI
    :<|> AttributeSearchAPI
    :<|> GetPackageAPI
    :<|> CreatePackageAPI
    :<|> UploadReleaseAPI
    :<|> GetRepositoriesAPI
    :<|> GetVersionFiles
    :<|> GetLatestVersionAPI

type BintrayManagementAPIWithAuth =
         BasicAuth "" BasicAuthData :> GetReleaseAttributesAPI
    :<|> BasicAuth "" BasicAuthData :> GetPackageAttributesAPI
    :<|> BasicAuth "" BasicAuthData :> UpdateReleaseAttributesAPI
    :<|> BasicAuth "" BasicAuthData :> UpdatePackageAttributesAPI
    :<|> BasicAuth "" BasicAuthData :> AttributeSearchAPI
    :<|> BasicAuth "" BasicAuthData :> GetPackageAPI
    :<|> BasicAuth "" BasicAuthData :> CreatePackageAPI
    :<|> BasicAuth "" BasicAuthData :> UploadReleaseAPI
    :<|> BasicAuth "" BasicAuthData :> GetRepositoriesAPI
    :<|> BasicAuth "" BasicAuthData :> GetVersionFiles
    :<|> BasicAuth "" BasicAuthData :> GetLatestVersionAPI

-- API to download packages from Bintray
-- E.g.: GET https://dl.bintray.com/:subject/:repo/:file_path

-- GET /:subject/:repo/:file_path
type DownloadAPI =
    Capture "subject" Ty.Subject
 :> Capture "repository" Ty.Repository
 :> CaptureAll "filepath" Text
 :> Get '[Ty.AcceptEverything] ByteString

type BintrayDownloadAPINoAuth =
    DownloadAPI

type BintrayDownloadAPIWithAuth =
    BasicAuth "" BasicAuthData :> DownloadAPI
