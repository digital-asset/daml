-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{- HLINT ignore "Avoid restricted extensions" -}
{-# LANGUAGE CPP             #-}
{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.JsonReader (
    allStablePackageIds
  , stablePackagesForVersion
  ) where

import qualified Data.Aeson           as Aeson
import           Data.Bifunctor
import           Data.ByteString      (ByteString)
import           Data.FileEmbed       (embedFile)
import qualified Data.Text            as T

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.Ast.Range as R

jsonData :: ByteString
jsonData = $(embedFile JSON_FILE_PATH)

decoded :: Either String [(Version, T.Text)]
decoded = Aeson.eitherDecodeStrict jsonData

entries :: [(VersionReq, PackageId)]
entries = case decoded of
  Left errMsg
    -> error $ "Failed to decode: " ++ errMsg
  Right parsedEntries -> map (bimap R.From PackageId) parsedEntries

allStablePackageIds :: [PackageId]
allStablePackageIds = map snd entries

stablePackagesForVersion :: Version -> [PackageId]
stablePackagesForVersion v = map snd $ filter (R.elem v . fst) entries
