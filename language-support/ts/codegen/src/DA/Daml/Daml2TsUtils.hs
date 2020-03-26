-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE DerivingStrategies #-}
module DA.Daml.Daml2TsUtils (
    NpmPackageName(..)
  , NpmPackageVersion(..)
  , writeRootPackageJson
  ) where

import qualified Data.Text.Extended as T
import qualified Data.ByteString.Lazy as BSL
import qualified Data.HashMap.Strict as HMS
import Data.Hashable
import Data.Aeson
import System.FilePath

newtype NpmPackageName = NpmPackageName {unNpmPackageName :: T.Text}
  deriving stock (Eq, Show)
  deriving newtype (Hashable, FromJSON, ToJSON, ToJSONKey)
newtype NpmPackageVersion = NpmPackageVersion {unNpmPackageVersion :: T.Text}
  deriving stock (Eq, Show)
  deriving newtype (Hashable, FromJSON, ToJSON)

-- The need for this utility comes up in at least the assistant
-- integration and daml2ts tests.
writeRootPackageJson :: Maybe FilePath -> [String] -> IO ()
writeRootPackageJson dir workspaces =
  BSL.writeFile (maybe "package.json" (</> "package.json") dir) $ encode $
  object
  [ "private" .= True
  , "workspaces" .= map T.pack workspaces
  , "resolutions" .= HMS.fromList ([("@daml/types", "file:daml-types")] :: [(T.Text, T.Text)])
                                ]
