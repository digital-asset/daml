-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Services.MeteringReportService (
    getMeteringReport, 
    MeteringReport(..), 
    isoTimeToTimestamp,
  ) where


import Data.Aeson ( KeyValue((.=)), ToJSON(..), object)
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

data MeteredApplication = MeteredApplication {
  application :: ApplicationId
, events :: Int64
} deriving (Show)

instance ToJSON MeteredApplication where
  toJSON (MeteredApplication application events) =
    object
    [   "application" .= unApplicationId application
    ,   "events" .= events
    ]

data MeteringReport = MeteringReport {
  participant :: ParticipantId
, from  :: Timestamp
, toRequested :: Maybe Timestamp
, toActual :: Timestamp
, applications :: [MeteredApplication]
} deriving (Show)

instance ToJSON MeteringReport where
  toJSON (MeteringReport participant from toRequested toActual applications) =
    object (
    [   "participant" .= unParticipantId participant
    ,   "from" .= timestampToIso8601 from
    ,   "toActual" .= timestampToIso8601 toActual
    ,   "applications" .= applications
    ]
    ++ maybeToList (fmap (("toRequested" .=) . timestampToIso8601) toRequested)
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

isoTimeToTimestamp :: IsoTime -> Timestamp
isoTimeToTimestamp iso = systemTimeToTimestamp $ System.utcToSystemTime $ unIsoTime iso

raiseApplicationMeteringReport :: LL.ApplicationMeteringReport -> Perhaps MeteredApplication
raiseApplicationMeteringReport (LL.ApplicationMeteringReport llApp events) = do
  application <- raiseApplicationId llApp
  return MeteredApplication {..}

raiseParticipantMeteringReport ::  LL.GetMeteringReportRequest ->  LL.ParticipantMeteringReport -> Perhaps MeteringReport
raiseParticipantMeteringReport (LL.GetMeteringReportRequest (Just llFrom) llTo _)  (LL.ParticipantMeteringReport llParticipantId (Just llToActual) llAppReports) = do
  participant <- raiseParticipantId llParticipantId
  from <- raiseTimestamp llFrom
  toRequested <- traverse raiseTimestamp llTo
  toActual <- raiseTimestamp llToActual
  applications <- raiseList raiseApplicationMeteringReport llAppReports
  return MeteringReport{..}

raiseParticipantMeteringReport _ response = Left $ Unexpected ("raiseParticipantMeteringReport unable to parse response: " <> show response)

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
