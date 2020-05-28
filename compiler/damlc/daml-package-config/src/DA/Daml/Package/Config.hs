-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

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
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as A
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
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
    , pModulePrefixes :: Map Ghc.UnitId Ghc.ModuleName
    -- ^ Map from unit ids to a prefix for all modules in that package.
    -- If this is specified, all modules from the package will be remapped
    -- under the given prefix.
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
    pModulePrefixes <- fromMaybe Map.empty <$> queryProjectConfig ["module-prefixes"] project
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

-- | Orphans because I’m too lazy to newtype everything.
instance A.FromJSON Ghc.ModuleName where
    parseJSON = A.withText "ModuleName" $ \t -> pure $ Ghc.mkModuleName (T.unpack t)

instance A.ToJSON Ghc.ModuleName where
    toJSON m = A.toJSON (Ghc.moduleNameString m)

instance A.FromJSON Ghc.UnitId where
    parseJSON = A.withText "UnitId" $ \t -> pure $ Ghc.stringToUnitId (T.unpack t)

instance A.FromJSONKey Ghc.UnitId where
    fromJSONKey = A.FromJSONKeyText $ \t -> Ghc.stringToUnitId (T.unpack t)

instance A.ToJSON Ghc.UnitId where
    toJSON unitId = A.toJSON (Ghc.unitIdString unitId)

instance A.ToJSONKey Ghc.UnitId where
    toJSONKey =
        A.ToJSONKeyText (T.pack . Ghc.unitIdString) (A.text . T.pack . Ghc.unitIdString)
