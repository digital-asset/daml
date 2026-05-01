-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE FlexibleInstances #-}

module DA.Daml.Project.Types
    ( module DA.Daml.Project.Types
    , module DA.Daml.Version.Types
    ) where

import DA.Daml.Version.Types

import qualified Data.Yaml as Y
import qualified Data.Text as T
import Data.Text (Text)
import Control.Exception.Safe

data ConfigError
    = ConfigFileInvalid Text Y.ParseException
    | ConfigFieldInvalid Text [Text] String
    | ConfigFieldMissing Text [Text]
    deriving (Show)

instance Exception ConfigError where
    displayException (ConfigFileInvalid name err) =
        concat ["Invalid ", T.unpack name, " config file: ", displayException err]
    displayException (ConfigFieldInvalid name path msg) =
        concat ["Invalid ", T.unpack (T.intercalate "." path)
            , " field in ", T.unpack name, " config: ", msg]
    displayException (ConfigFieldMissing name path) =
        concat ["Missing required ", T.unpack (T.intercalate "." path)
            , " field in ", T.unpack name, " config."]

newtype PackageConfig = PackageConfig
    { unwrapPackageConfig :: Y.Value
    } deriving (Eq, Show, Y.FromJSON)

newtype MultiPackageConfig = MultiPackageConfig
    { unwrapMultiPackageConfig :: Y.Value
    } deriving (Eq, Show, Y.FromJSON)

-- | File path to a cache directory, e.g. ~/.cache.
newtype CachePath = CachePath
    { unwrapCachePath :: FilePath
    } deriving (Eq, Show)

-- | File path of package root.
newtype PackagePath = PackagePath
    { unwrapPackagePath :: FilePath
    } deriving (Eq, Show)
