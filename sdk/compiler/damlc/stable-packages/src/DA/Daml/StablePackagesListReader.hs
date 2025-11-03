-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{- This file parses stable-packages.yaml, passed in as YAML_FILE_PATH by Bazel via
 CPP. This list is used to solve the circular dependency between the encoder,
 which must know which Ids are stable, and the stable packages generator, which
 uses the encoder. See the documentation in stable-packages.yaml for further
 information. -}

{- HLINT ignore "Avoid restricted extensions" -}
{-# LANGUAGE CPP             #-}
{-# LANGUAGE TemplateHaskell #-}

module DA.Daml.StablePackagesListReader (
    allStablePackageIds
  , stablePackagesForVersion
  ) where

import           Data.Bifunctor
import qualified Data.Text            as T

import           DA.Daml.LF.Ast
import qualified DA.Daml.LF.Ast.Range as R

import           DA.Daml.EmbedFileWithDecoder

decoded :: [(Version, T.Text)]
decoded = $(embedFile)

entries :: [(VersionReq, PackageId)]
entries = map (bimap R.From PackageId) decoded

allStablePackageIds :: [PackageId]
allStablePackageIds = map snd entries

stablePackagesForVersion :: Version -> [PackageId]
stablePackagesForVersion v = map snd $ filter (R.elem v . fst) entries
