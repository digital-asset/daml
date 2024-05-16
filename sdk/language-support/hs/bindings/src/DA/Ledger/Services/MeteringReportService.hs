-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Services.MeteringReportService (
    getMeteringReport, 
    MeteringRequestByDay(..),
    timestampToIso8601,
    utcDayToTimestamp,
    toRawStructValue,
    toRawAesonValue
  ) where

import Data.Aeson ( KeyValue((.=)), ToJSON(..), object )
import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import qualified Data.Text.Lazy as TL
import Network.GRPC.HighLevel.Generated
import qualified Com.Daml.Ledger.Api.V2.Admin.MeteringReportService as LL
import Data.Maybe (maybeToList)
import qualified Data.Time.Clock.System as System
import qualified Data.Time.Format.ISO8601 as ISO8601
import GHC.Int (Int64)
import GHC.Word (Word32)
import Data.Time.Calendar (Day(..))
import Data.Time.Clock (secondsToDiffTime, UTCTime(..))
import qualified Data.Aeson as A
import qualified Data.Aeson.KeyMap as A
import qualified Data.Aeson.Key as A
import qualified Google.Protobuf.Struct as S
import qualified Data.Map as Map
import qualified Data.Scientific as Scientific

data MeteringRequestByDay = MeteringRequestByDay {
  from :: Day
, to :: Maybe Day
, application :: Maybe ApplicationId
} deriving (Show, Eq)

instance ToJSON MeteringRequestByDay where
  toJSON (MeteringRequestByDay from to application) =
    object (
    [
      "from" .= show from
    ]
    ++ maybeToList (fmap (("to" .=) . show) to)
    ++ maybeToList (fmap (("application" .=) . unApplicationId) application)
    )

timestampToSystemTime :: Timestamp -> System.SystemTime
timestampToSystemTime ts = st
  where
    s = fromIntegral (seconds ts) :: Int64
    n = fromIntegral (nanos ts) :: Word32
    st = System.MkSystemTime s n

systemTimeToTimestamp :: System.SystemTime -> Timestamp
systemTimeToTimestamp st = ts
  where
    s = fromIntegral (System.systemSeconds st) :: Int
    n = fromIntegral (System.systemNanoseconds st) :: Int
    ts = Timestamp s n

timestampToIso8601 :: Timestamp -> String
timestampToIso8601 ts = ISO8601.iso8601Show ut
  where
    st = timestampToSystemTime ts
    ut = System.systemToUTCTime st

utcDayToTimestamp :: Day -> Timestamp
utcDayToTimestamp day = systemTimeToTimestamp $ System.utcToSystemTime $ UTCTime day (secondsToDiffTime 0)

kindToAesonValue :: S.ValueKind -> A.Value
kindToAesonValue (S.ValueKindStructValue (S.Struct sMap)) =
  A.Object aMap
  where
    aMap = A.fromMap $ s2a <$> Map.mapKeys (A.fromText . TL.toStrict) sMap
    s2a (Just v) = toRawAesonValue v
    s2a Nothing = A.Null

kindToAesonValue (S.ValueKindListValue (S.ListValue list)) = A.Array $ fmap toRawAesonValue list
kindToAesonValue (S.ValueKindStringValue text) = A.String (TL.toStrict text)
kindToAesonValue (S.ValueKindNullValue _) = A.Null
kindToAesonValue (S.ValueKindNumberValue num) = A.Number $ Scientific.fromFloatDigits num
kindToAesonValue (S.ValueKindBoolValue b) = A.Bool b

toRawAesonValue :: S.Value -> A.Value
toRawAesonValue (S.Value (Just kind)) = kindToAesonValue kind
toRawAesonValue (S.Value Nothing) = A.Null

toRawStructValue :: A.Value -> S.Value
toRawStructValue (A.Object aMap) =
    S.Value $ Just $ S.ValueKindStructValue $ S.Struct sMap
    where
      sMap = Map.mapKeys TL.fromStrict $ fmap a2s $ A.toMapText aMap
      a2s v = Just $ toRawStructValue v

toRawStructValue (A.Array array) = S.Value $ Just $ S.ValueKindListValue $ S.ListValue $ fmap toRawStructValue array
toRawStructValue (A.String text) = S.Value $ Just $ S.ValueKindStringValue (TL.fromStrict text)
toRawStructValue (A.Number num) = S.Value $ Just $ S.ValueKindNumberValue $ Scientific.toRealFloat num
toRawStructValue (A.Bool b) = S.Value $ Just $ S.ValueKindBoolValue b
toRawStructValue A.Null = S.Value Nothing

raiseGetMeteringReportResponse :: LL.GetMeteringReportResponse -> Perhaps A.Value
raiseGetMeteringReportResponse (LL.GetMeteringReportResponse _ _ (Just reportStruct)) =
  Right $ toRawAesonValue $ S.Value $ Just $ S.ValueKindStructValue reportStruct
raiseGetMeteringReportResponse response = Left $ Unexpected ("raiseMeteredReport unable to parse response: " <> show response)

getMeteringReport :: Timestamp -> Maybe Timestamp -> Maybe ApplicationId -> LedgerService A.Value
getMeteringReport from to applicationId =
    makeLedgerService $ \timeout config mdm ->
    withGRPCClient config $ \client -> do
        service <- LL.meteringReportServiceClient client
        let
          LL.MeteringReportService {meteringReportServiceGetMeteringReport=rpc} = service
          gFrom = Just $ lowerTimestamp from
          gTo = fmap lowerTimestamp to
          gApp = maybe TL.empty unApplicationId applicationId
          request = LL.GetMeteringReportRequest gFrom gTo gApp
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrap
            >>= either (fail . show) return . raiseGetMeteringReportResponse
