-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DerivingStrategies #-}
module Migration.Types
  ( Test(..)
  , SdkVersion(..)
  , ContractId(..)
  , Party(..)
  , Tuple2(..)
  ) where

import qualified Data.Aeson as A
import qualified Data.Text as T
import GHC.Generics (Generic)

data Test s r = Test
  { executeStep :: SdkVersion -> String -> Int -> s -> IO r
  -- ^ Execute a step given SDK version, the host and port
  -- and current state and produce a result.
  , validateStep :: SdkVersion -> s -> r -> Either String s
  -- ^ Validate the result of a step given the SDK version and current state
  -- and produce either an error or a new state.
  , initialState :: s
  -- ^ The initial state to start with.
  }

newtype SdkVersion = SdkVersion { getSdkVersion :: String }

-- The datatypes are defined such that the autoderived Aeson instances
-- match the DAML-LF JSON encoding.

newtype ContractId t = ContractId T.Text
  deriving newtype A.FromJSON
  deriving stock (Eq, Show)
newtype Party = Party { getParty :: T.Text }
  deriving newtype (A.FromJSON, A.ToJSON)
  deriving stock (Eq, Show)

data Tuple2 a b = Tuple2
  { _1 :: a
  , _2 :: b
  } deriving (Eq, Generic, Show)

instance (A.FromJSON a, A.FromJSON b) => A.FromJSON (Tuple2 a b)