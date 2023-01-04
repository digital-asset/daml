-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DerivingStrategies #-}
module Migration.Types
  ( Test(..)
  , interleave
  , SdkVersion(..)
  , getSdkVersion
  , ContractId(..)
  , Party(..)
  , Tuple2(..)
  , TemplateId(..)
  , Event(..)
  , Transaction(..)
  ) where

import qualified Data.Aeson as A
import qualified Data.Text as T
import qualified Data.Int as I
import qualified Data.SemVer as SemVer
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

interleave :: Test s r -> Test s' r' -> Test (s, s') (r, r')
interleave t t' = Test
    { executeStep = \ver host port (s, s') -> (,)
        <$> executeStep t ver host port s
        <*> executeStep t' ver host port s'
    , validateStep = \ver (s, s') (r, r') -> (,)
        <$> validateStep t ver s r
        <*> validateStep t' ver s' r'
    , initialState = (initialState t, initialState t')
    }

newtype SdkVersion = SdkVersion SemVer.Version

getSdkVersion :: SdkVersion -> String
getSdkVersion (SdkVersion ver) = SemVer.toString ver

-- The datatypes are defined such that the autoderived Aeson instances
-- match the Daml-LF JSON encoding.

newtype ContractId = ContractId T.Text
  deriving newtype A.FromJSON
  deriving stock (Eq, Show, Ord)
newtype Party = Party { getParty :: T.Text }
  deriving newtype (A.FromJSON, A.ToJSON)
  deriving stock (Eq, Show, Ord)

data Tuple2 a b = Tuple2
  { _1 :: a
  , _2 :: b
  } deriving (Eq, Generic, Show, Ord)

instance (A.FromJSON a, A.FromJSON b) => A.FromJSON (Tuple2 a b)

-- | Since this is only for testing, we omit the package id for convenience.
data TemplateId = TemplateId
  { moduleName :: !String
  , entityName :: !String
  } deriving (Eq, Show)

data Event
  = Created !ContractId !TemplateId !A.Value
  | Archived !ContractId !TemplateId
  deriving (Eq, Show)

instance A.FromJSON Event where
    parseJSON = A.withObject "Event" $ \o -> do
        ty :: String <- o A..: "type"
        tplId <- TemplateId
            <$> o A..: "moduleName"
            <*> o A..: "entityName"
        cid <- o A..: "contractId"
        case ty of
            "created" -> Created cid tplId <$> o A..: "argument"
            "archived" -> pure (Archived cid tplId)
            _ -> fail $ "Unsupported type: " <> show ty

data Transaction = Transaction
  { transactionId :: T.Text
  , events :: [Event]
  , letMicros :: !I.Int64    -- :: Ledger effective time, in microseconds since unix epoch
  } deriving (Generic, Eq, Show)

instance A.FromJSON Transaction
