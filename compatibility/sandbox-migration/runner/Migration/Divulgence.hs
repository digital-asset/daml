-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
module Migration.Divulgence (test) where

import Control.Monad
import Control.Monad.Except
import qualified Data.Aeson as A
import Data.List
import qualified Data.Text as T
import GHC.Generics (Generic)
import System.IO.Extra
import System.Process

import Migration.Types
import Migration.Util

test :: FilePath -> FilePath -> Test ([Tuple2 ContractId Asset], [Tuple2 ContractId Asset]) Result
test step modelDar = Test {..}
  where
    initialState = ([], [])
    executeStep sdkVersion host port _state = withTempFile $ \outputFile -> do
        let suffix = getSdkVersion sdkVersion
        callProcess step
            [ "--host=" <> host
            , "--port=" <> show port
            , "--output", outputFile
            , "--dar=" <> modelDar
            , "--test=divulgence," <> T.unpack (getParty testOwner) <> "," <> T.unpack (getParty testDivulgee) <> "," <> suffix
            ]
        either fail pure =<< A.eitherDecodeFileStrict' outputFile
    validateStep sdkVersion (prevAssets, prevDivulgedAssets) Result {..} = do
        let suffix = getSdkVersion sdkVersion
        unless (equivalent oldAssets prevAssets) $
            throwError ("The old private assets do not match those returned by the previous run: " <> show oldAssets)
        unless (equivalent oldDivulgedAssets prevDivulgedAssets) $
            throwError ("The old divulged assets do not match those returned by the previous run: " <> show oldDivulgedAssets)
        let assetsDiff = map _2 newAssets \\ map _2 oldAssets
        unless (equivalent assetsDiff [Asset testOwner ("private-" <> suffix), Asset testOwner ("divulging-" <> suffix)]) $
            throwError ("Expected one private and one divulged contract, got " <> show assetsDiff)
        let divulgedAssetDiff = map _2 newDivulgedAssets \\ map _2 oldDivulgedAssets
        unless (divulgedAssetDiff == [Asset testOwner ("divulging-" <> suffix)]) $
            throwError ("Expected one divulged contract, got " <> show divulgedAssetDiff)
        pure (newAssets, newDivulgedAssets)

    testOwner :: Party
    testOwner = Party "owner"
    testDivulgee :: Party
    testDivulgee = Party "divulgee"

-- The datatypes are defined such that the autoderived Aeson instances
-- match the Daml-LF JSON encoding.
--
data Asset = Asset
  { owner :: Party
  , tag :: String
  } deriving (Eq, Generic, Show, Ord)

instance A.FromJSON Asset

data Result = Result
  { oldAssets :: [Tuple2 ContractId Asset]
  , newAssets :: [Tuple2 ContractId Asset]
  , oldDivulgedAssets :: [Tuple2 ContractId Asset]
  , newDivulgedAssets :: [Tuple2 ContractId Asset]
  } deriving Generic

instance A.FromJSON Result
