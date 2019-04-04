-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}

module DA.Sdk.Cli.Credentials
    ( Credentials (..)
    , BintrayCredentials (..)
    ) where

import DA.Sdk.Prelude

-- | Bintray Credentials
data BintrayCredentials = BintrayCredentials
    { bintrayUsername :: Text
    , bintrayKey :: Text
    } deriving (Show, Eq)

-- | Credentials
newtype Credentials = Credentials
    { bintrayCredentials :: Maybe BintrayCredentials
    } deriving (Show, Eq)
