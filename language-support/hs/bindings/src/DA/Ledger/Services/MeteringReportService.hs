-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Services.MeteringReportService (
    getMeteringReport, 
    MeteringReport(..),
    MeteringRequest(..),
    MeteringRequestByDay(..),
    MeteredApplication(..),
    isoTimeToTimestamp,
    timestampToIso8601,
    utcDayToTimestamp,
  ) where

import Data.Aeson ( KeyValue((.=)), ToJSON(..), FromJSON(..), object, withObject, (.:), (.:?) )
import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Types
import qualified Data.Text.Lazy as TL
import Network.GRPC.HighLevel.Generated
import qualified Com.Daml.Ledger.Api.V1.Admin.MeteringReportService as LL
import Data.Maybe (maybeToList)
import qualified Data.Time.Clock.System as System
import qualified Data.Time.Format.ISO8601 as ISO8601
import GHC.Int (Int64)
import GHC.Word (Word32)
import Data.Time.Calendar (Day(..))
import Data.Time.Clock (secondsToDiffTime, UTCTime(..))

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

data MeteringRequest = MeteringRequest {
  from :: Timestamp
, to :: Maybe Timestamp
, application :: Maybe ApplicationId
} deriving (Show, Eq)

instance ToJSON MeteringRequest where
  toJSON (MeteringRequest from to application) =
    object (
    [
      "from" .= timestampToIso8601 from
    ]
    ++ maybeToList (fmap (("to" .=) . timestampToIso8601) to)
    ++ maybeToList (fmap (("application" .=) . unApplicationId) application)
    )

instance FromJSON MeteringRequest where
    parseJSON = withObject "MeteringRequest" $ \v -> MeteringRequest
        <$> fmap isoTimeToTimestamp (v .: "from")
        <*> fmap (\o -> fmap isoTimeToTimestamp o) (v .:? "to")
        <*> v .:? "application"

data MeteredApplication = MeteredApplication {
  application :: ApplicationId
, events :: Int64
} deriving (Show, Eq)

instance ToJSON MeteredApplication where
  toJSON (MeteredApplication application events) =
    object
    [   "application" .= unApplicationId application
    ,   "events" .= events
    ]

instance FromJSON MeteredApplication where
    parseJSON = withObject "MeteredApplication" $ \v -> MeteredApplication
        <$> v .: "application"
        <*> v .: "events"

data MeteringReport = MeteringReport {
  participant :: ParticipantId
, request  :: MeteringRequest
, isFinal :: Bool
, applications :: [MeteredApplication]
} deriving (Show, Eq)

instance ToJSON MeteringReport where
  toJSON (MeteringReport participant request isFinal applications) =
    object
    [   "participant" .= unParticipantId participant
    ,   "request" .= request
    ,   "final" .= isFinal
    ,   "applications" .= applications
    ]

instance FromJSON MeteringReport where
    parseJSON = withObject "MeteringReport" $ \v -> MeteringReport
        <$> v .: "participant"
        <*> v .: "request"
        <*> v .: "final"
        <*> v .: "applications"

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

isoTimeToTimestamp :: IsoTime -> Timestamp
isoTimeToTimestamp iso = systemTimeToTimestamp $ System.utcToSystemTime $ unIsoTime iso

utcDayToTimestamp :: Day -> Timestamp
utcDayToTimestamp day = systemTimeToTimestamp $ System.utcToSystemTime $ UTCTime day (secondsToDiffTime 0)

raiseApplicationMeteringReport :: LL.ApplicationMeteringReport -> Perhaps MeteredApplication
raiseApplicationMeteringReport (LL.ApplicationMeteringReport llApp events) = do
  application <- raiseApplicationId llApp
  return MeteredApplication {..}

raiseGetMeteringReportRequest ::  LL.GetMeteringReportRequest -> Perhaps MeteringRequest
raiseGetMeteringReportRequest (LL.GetMeteringReportRequest (Just llFrom) llTo llApplication) = do
  from <- raiseTimestamp llFrom
  to <- traverse raiseTimestamp llTo
  let maybeApplication = if TL.null llApplication then Nothing else Just llApplication
  application <- traverse raiseApplicationId maybeApplication
  return MeteringRequest{..}

raiseGetMeteringReportRequest response = Left $ Unexpected ("raiseGetMeteringReportRequest unable to parse response: " <> show response)

raiseParticipantMeteringReport ::  LL.GetMeteringReportRequest ->  LL.ParticipantMeteringReport -> Perhaps MeteringReport
raiseParticipantMeteringReport llRequest  (LL.ParticipantMeteringReport llParticipantId isFinal llAppReports) = do
  participant <- raiseParticipantId llParticipantId
  request <- raiseGetMeteringReportRequest llRequest
  applications <- raiseList raiseApplicationMeteringReport llAppReports
  return MeteringReport{..}

raiseGetMeteringReportResponse :: LL.GetMeteringReportResponse -> Perhaps MeteringReport
raiseGetMeteringReportResponse (LL.GetMeteringReportResponse (Just request) (Just report) (Just _)) =
  raiseParticipantMeteringReport request report

raiseGetMeteringReportResponse response = Left $ Unexpected ("raiseMeteredReport unable to parse response: " <> show response)

getMeteringReport :: Timestamp -> Maybe Timestamp -> Maybe ApplicationId -> LedgerService MeteringReport
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
