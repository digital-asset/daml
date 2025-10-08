-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}

module DA.Daml.LF.Ast.Version.VersionType(module DA.Daml.LF.Ast.Version.VersionType) where

import           Control.DeepSeq
import           Data.Data
import           GHC.Generics

-- | Daml-LF version of an archive payload.
data Version = Version
    { versionMajor :: MajorVersion
    , versionMinor :: MinorVersion
    }
    deriving (Eq, Data, Generic, NFData, Show)

data MajorVersion = V2
  deriving (Eq, Data, Generic, NFData, Ord, Show, Enum, Bounded)

data MinorVersion = PointStable Int | PointStaging Int | PointDev
  deriving (Eq, Data, Generic, NFData, Ord, Show)
