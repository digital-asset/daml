-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell   #-}

module DA.Sdk.Cli.Template.Types
    ( tiPath
    , TemplateInfo(..)
    ) where

import Data.Text
import Control.Lens     (makeLenses)
import DA.Sdk.Prelude
import DA.Sdk.Cli.Repository.Types

data TemplateInfo = TemplateInfo
    { tiName :: Text
    , _tiPath :: Maybe FilePath
    , tiVersion :: Text
    , tiDescription :: Text
    , tiType        :: TemplateType
    } deriving (Show, Eq)

makeLenses ''TemplateInfo
