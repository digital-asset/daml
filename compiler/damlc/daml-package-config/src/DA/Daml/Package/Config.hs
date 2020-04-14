-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Types and functions for dealing with package config in daml.yaml
module DA.Daml.Package.Config
    ( PackageConfigFields (..)
    , PackageSdkVersion (..)
    , parseProjectConfig
    , overrideSdkVersion
    , withPackageConfig
    ) where

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import SdkVersion

import Control.Exception.Safe (throwIO)
import Control.Monad (when)
import Data.Maybe (fromMaybe)
import qualified Data.Yaml as Y
import qualified Module as Ghc
import System.IO (hPutStrLn, stderr)

-- | daml.yaml config fields specific to packaging.
data PackageConfigFields = PackageConfigFields
    { pName :: LF.PackageName
    , pSrc :: String
    , pExposedModules :: Maybe [Ghc.ModuleName]
    , pVersion :: Maybe LF.PackageVersion
    -- ^ This is optional since for `damlc compile` and `damlc package`
    -- we might not have a version. In `damlc build` this is always set to `Just`.
    , pDependencies :: [String]
    , pDataDependencies :: [String]
    , pSdkVersion :: PackageSdkVersion
    }

-- | SDK version for package.
newtype PackageSdkVersion = PackageSdkVersion
    { unPackageSdkVersion :: String
    } deriving (Eq, Y.FromJSON)

-- | Parse the daml.yaml for package specific config fields.
parseProjectConfig :: ProjectConfig -> Either ConfigError PackageConfigFields
parseProjectConfig project = do
    pName <- queryProjectConfigRequired ["name"] project
    pSrc <- queryProjectConfigRequired ["source"] project
    pExposedModules <-
        fmap (map Ghc.mkModuleName) <$>
        queryProjectConfig ["exposed-modules"] project
    pVersion <- Just <$> queryProjectConfigRequired ["version"] project
    pDependencies <- queryProjectConfigRequired ["dependencies"] project
    pDataDependencies <- fromMaybe [] <$> queryProjectConfig ["data-dependencies"] project
    pSdkVersion <- queryProjectConfigRequired ["sdk-version"] project
    Right PackageConfigFields {..}

overrideSdkVersion :: PackageConfigFields -> IO PackageConfigFields
overrideSdkVersion pkgConfig = do
    sdkVersionM <- getSdkVersionMaybe
    case sdkVersionM of
        Nothing ->
            pure pkgConfig
        Just sdkVersion -> do
            when (pSdkVersion pkgConfig /= PackageSdkVersion sdkVersion) $
                hPutStrLn stderr $ unwords
                    [ "Warning: Using DAML SDK version"
                    , sdkVersion
                    , "from"
                    , sdkVersionEnvVar
                    , "enviroment variable instead of DAML SDK version"
                    , unPackageSdkVersion (pSdkVersion pkgConfig)
                    , "from"
                    , projectConfigName
                    , "config file."
                    ]
            pure pkgConfig { pSdkVersion = PackageSdkVersion sdkVersion }

--- | replace SDK version with one ghc-pkg accepts
---
--- This should let release version unchanged, but convert snapshot versions.
--- See module SdkVersion (in //BUILD) for details.
replaceSdkVersionWithGhcPkgVersion :: PackageConfigFields -> PackageConfigFields
replaceSdkVersionWithGhcPkgVersion p@PackageConfigFields{ pSdkVersion = PackageSdkVersion v } =
    p { pSdkVersion = PackageSdkVersion $ SdkVersion.toGhcPkgVersion v }

withPackageConfig :: ProjectPath -> (PackageConfigFields -> IO a) -> IO a
withPackageConfig projectPath f = do
    project <- readProjectConfig projectPath
    pkgConfig <- either throwIO pure (parseProjectConfig project)
    pkgConfig' <- overrideSdkVersion pkgConfig
    let pkgConfig'' = replaceSdkVersionWithGhcPkgVersion pkgConfig'
    f pkgConfig''
