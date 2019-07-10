-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE GADTs #-}
{-|
This uses the Google Cloud Platform (GCP) Stackdriver logging service to store
log information. It builds up a queue of messages and attempts to send those
messages in batches once the batches are full. If there isn't a connection to
the backend the messages will be stored in memory until a connection can be
made. These logs are not persisted on disk, the only thing that is persisted
is an estimate of the amount of data sent over the network. Once the data sent
reaches a limit it will stop being sent. This will retry whenever a new message
is added.
-}
module DA.Service.Logger.Impl.GCP
    ( withGcpLogger
    , GCPState(..)
    , initialiseGcpState
    , logOptOut
    , SendResult(..)
    , isSuccess
    , sendData
    , sentDataFile
    -- * Test hooks
    , test
    ) where

import GHC.Generics(Generic)
import Data.Int
import Data.Tuple
import Text.Read(readMaybe)
import Data.Aeson as Aeson
import Control.Monad
import Control.Monad.IO.Class
import GHC.Stack
import System.Directory
import System.Environment
import System.FilePath
import System.Info
import System.Timeout
import System.Random
import qualified DA.Service.Logger as Lgr
import qualified DA.Service.Logger.Impl.Pure as Lgr.Pure
import DAML.Project.Consts
import qualified Data.HashMap.Strict as HM
import qualified Data.Text.Extended as T
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import Data.Time as Time
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Control.Concurrent.Extra
import Control.Exception.Safe
import Network.HTTP.Simple

-- Type definitions

data GCPState = GCPState
    { gcpFallbackLogger :: Lgr.Handle IO
    -- ^ Fallback logger to log exceptions caused by the GCP logging itself.
    , gcpLogQueue :: Var [LogEntry]
    -- ^ Unsent logs with the oldest log entry first.
    , gcpSessionID :: UUID
    -- ^ Identifier for the current session
    , gcpDamlDir :: FilePath
    -- ^ Directory where we store various files such as the amount of
    -- data sent so far.
    , gcpSentDataFileLock :: Lock
    -- ^ Lock for accessing sendData
    -- TODO (MK) This doesn’t actually work for concurrent executions.
    }

newtype SentBytes = SentBytes { getSentBytes :: Int64 }
    deriving (Eq, Ord, Num)

data SentData = SentData
    { date :: Time.Day
    , sent :: SentBytes
    }

-- Files used to record data that should persist over restarts.

sentDataFile :: GCPState -> FilePath
sentDataFile GCPState{gcpDamlDir} = gcpDamlDir </> ".sent_data"

machineIDFile :: GCPState -> FilePath
machineIDFile GCPState{gcpDamlDir} = gcpDamlDir </> ".machine_id"

optedOutFile :: GCPState -> FilePath
optedOutFile GCPState{gcpDamlDir} = gcpDamlDir </> ".opted_out"

noSentData :: IO SentData
noSentData = SentData <$> today <*> pure (SentBytes 0)

showSentData :: SentData -> T.Text
showSentData SentData{..} = T.unlines [T.pack $ show date, T.pack $ show $ getSentBytes sent]

-- | Reads the data file but doesn't check the values are valid
parseSentData :: T.Text -> Maybe SentData
parseSentData s = do
  (date',sent') <-
    case T.lines s of
      [date, sent] -> Just (T.unpack date, T.unpack sent)
      _ -> Nothing
  date <- readMaybe date'
  sent <- SentBytes <$> readMaybe sent'
  pure SentData{..}

-- | Resets to an empty data file if the recorded data
-- is not from today.
validateSentData :: Maybe SentData -> IO SentData
validateSentData = \case
  Nothing -> noSentData
  Just sentData -> do
    today' <- today
    if date sentData == today'
      then pure sentData
      else noSentData

initialiseGcpState :: Lgr.Handle IO -> IO GCPState
initialiseGcpState lgr =
    GCPState lgr
        <$> newVar []
        <*> randomIO
        <*> getDamlDir
        <*> newLock


-- | retains the normal logging capacities of the handle but adds logging
--   to GCP
--   will attempt to flush the logs
withGcpLogger ::
    HasCallStack =>
    (Lgr.Priority -> Bool) -- ^ if this is true log to GCP
    -> Lgr.Handle IO
    -> (Lgr.Handle IO -> IO a)
    -> IO a
withGcpLogger p hnd f =
    bracket
      init
      (\(gcp,_) -> flushLogsGCP gcp Lgr.Info logsFlushed) $ f . snd
  where

  init :: HasCallStack => IO (GCPState, Lgr.Handle IO)
  init = do
      gcpState <- initialiseGcpState hnd
      let logger = hnd{Lgr.logJson = newLogJson gcpState}
      -- this is here because it should be sent regardless of filtering
      -- TODO (MK) This is a bad idea, we block every startup on this.
      logGCP gcpState Lgr.Info =<< metaData gcpState
      pure (gcpState, logger)

  logsFlushed :: T.Text
  logsFlushed = "Logs have been flushed"

  newLogJson ::
      HasCallStack =>
      Aeson.ToJSON a =>
      GCPState -> Lgr.Priority -> a -> IO ()
  newLogJson gcp priority js = do
    Lgr.logJson hnd priority js
    when (p priority) $
      logGCP gcp priority js

data LogEntry = LogEntry
    { severity :: !Lgr.Priority
    , timeStamp :: !UTCTime
    , message :: !(WithSession Value)
    }

instance ToJSON LogEntry where
    toJSON LogEntry{..} = Object $ HM.fromList
        [ priorityToGCP severity
        , ("timestamp", toJSON timeStamp)
        , ("jsonPayload", toJSON message)
        ]

-- | this stores information about the users machine and is transmitted at the
--   start of every session
data MetaData = MetaData
    { machineID :: !UUID
    , operatingSystem :: !T.Text
    , version :: !(Maybe T.Text)
    } deriving Generic
instance ToJSON MetaData

metaData :: GCPState -> IO MetaData
metaData gcp = do
    machineID <- fetchMachineID gcp
    v <- lookupEnv sdkVersionEnvVar
    let version = case v of
            Nothing -> Nothing
            Just "" -> Nothing
            Just vs -> Just $ T.pack vs
    pure MetaData
        { machineID
        , operatingSystem=T.pack os
        , version
        }

-- | This associates the message payload with an individual session and the meta
data WithSession a = WithSession {wsID :: UUID, wsContents :: a}

instance ToJSON a => ToJSON (WithSession a) where
    toJSON WithSession{..} =
        Object $
            HM.insert "SESSION_ID" (toJSON wsID) $
            toJsonObject $
            toJSON wsContents

createLog
    :: (Aeson.ToJSON a, HasCallStack)
    => GCPState -> Lgr.Priority -> a -> IO LogEntry
createLog GCPState{gcpSessionID} severity m = do
    let message = WithSession gcpSessionID $ toJSON m
    timeStamp <- getCurrentTime
    pure LogEntry{..}

-- | This will turn non objects into objects with a key of their type
--   e.g. 1e100 -> {"Number" : 1e100}, keys are "Array", "String", "Number"
--   "Bool" and "Null"
--   it will return objects unchanged
--   The payload must be a JSON object
-- TODO (MK) This encoding is stupid and wrong but we might have some queries
-- that rely on it, so for now let’s keep it.
toJsonObject :: Aeson.Value
             -> HM.HashMap T.Text Value
toJsonObject v = either (\k -> HM.singleton k v) id $ objectOrKey v where
  objectOrKey = \case
    Aeson.Object o -> Right o
    Aeson.Array _ -> Left "Array"
    Aeson.String _ -> Left "String"
    Aeson.Number _ -> Left "Number"
    Aeson.Bool _ -> Left "Bool"
    Aeson.Null -> Left "Null"

priorityToGCP :: Lgr.Priority -> (T.Text, Value)
priorityToGCP prio = ("severity",prio')
    where
        prio' = case prio of
            Lgr.Error -> "ERROR"
            Lgr.Warning -> "WARNING"
            Lgr.Info -> "INFO"
            Lgr.Debug -> "DEBUG"

-- | This attempts to send all the logs off to the server but will give up
--   after 5 seconds
flushLogsGCP
    :: Aeson.ToJSON a
    => GCPState
    -> Lgr.Priority
    -> a
    -- ^ explain why you're flushing the logs e.g. crash, shutdown ect
    -> IO ()
flushLogsGCP gcp priority js = do
    le <- createLog gcp priority js
    pushLogQueue gcp [le]
    void $ sendLogQueue gcp Sync

minBatchSize, maxBatchSize :: Int
minBatchSize = 10
maxBatchSize = 50

-- | Add something to the log queue.
-- If we reached the batch size, we will publish the logs.
logGCP
    :: Aeson.ToJSON a
    => GCPState
    -> Lgr.Priority
    -> a
    -> IO ()
logGCP gcp priority js = do
    le <- createLog gcp priority js
    pushLogQueue gcp [le]
    len <- logQueueLength gcp
    when (len >= minBatchSize) $
        sendLogQueue gcp Async

-- | try sending logs and return the ones that weren't sent
--   don't use this directly, instead use sendLogQueue
sendLogs :: GCPState -> [LogEntry] -> IO [LogEntry]
sendLogs gcp contents = do
    res <- sendData gcp (void . httpNoBody . toRequest) $ encode contents
    case res of
        -- The limit has been reached so don't re-add anything
        ReachedDataLimit -> pure []
        -- Sending failed so re-add
        -- TODO (MK) This is a bad idea, the most likely cause
        -- for this is that we are behind a firewall so we will
        -- repeatedly fail and keep increasing memory usage.
        -- Given that logs are not critical, we a more sensible
        -- option would probably be to
        HttpError e -> do
            logException gcp e
            pure contents
        SendSuccess -> pure []

logsHost :: BS.ByteString
logsHost = "logs.daml.com"

toRequest :: LBS.ByteString -> Request
toRequest le =
      setRequestMethod "POST"
    $ setRequestSecure True
    $ setRequestHost logsHost
    $ setRequestPort 443
    $ setRequestPath "log/ide/"
    $ addRequestHeader "Content-Type" "application/json; charset=utf-8"
    $ setRequestBodyLBS le
    $ setRequestCheckStatus
      defaultRequest

pushLogQueue ::
    GCPState ->
    [LogEntry] ->
    IO ()
pushLogQueue GCPState{..} logs =
    modifyVar_ gcpLogQueue (pure . (logs ++))

popLogQueue ::
    GCPState ->
    IO [LogEntry]
popLogQueue GCPState{..} =
    modifyVar gcpLogQueue $ pure . swap . splitAt maxBatchSize

logQueueLength ::
    GCPState ->
    IO Int
logQueueLength GCPState{..} =
    withVar gcpLogQueue (pure . length)

data RunSync =
    Async
  | Sync

sendLogQueue ::
    GCPState ->
    RunSync ->
    IO ()
sendLogQueue gcp@GCPState{..} runSync = do
  toSend <- popLogQueue gcp
  let send = do
          remainder <- sendLogs gcp toSend
          pushLogQueue gcp remainder
  case runSync of
      Sync -> void $ timeout 5_000_000 send
      Async ->
          -- TODO (MK) This is a bad idea, we should
          -- just have a separate thread that process log requests.
          void $ forkIO send

-- | We log exceptions at Info priority instead of Error since the most likely cause
-- is a firewall.
logException :: Exception e => GCPState -> e -> IO ()
logException GCPState{gcpFallbackLogger} e = Lgr.logJson gcpFallbackLogger Lgr.Info $ displayException e

-- | This machine ID is created once and read at every subsequent startup
--   it's a random number which is used to identify machines
fetchMachineID :: GCPState -> IO UUID
fetchMachineID gcp = do
    let fp = machineIDFile gcp
    let generateID = do
        mID <- randomIO
        T.writeFileUtf8 fp $ UUID.toText mID
        pure mID
    exists <- doesFileExist fp
    if exists
       then do
        uid <- UUID.fromText <$> T.readFileUtf8 fp
        maybe generateID pure uid
       else
        generateID

-- | If it hasn't already been done log that the user has opted out of telemetry.
-- TODO (MK) This is stupid for two reasons:
-- 1. It tries to send the log synchronously which is a stupid idea
-- 2. It doesn’t go through withGcpLogger for no good reason.
logOptOut :: Lgr.Handle IO -> IO ()
logOptOut hnd = do
    gcp <- initialiseGcpState hnd
    let fp = optedOutFile gcp
    exists <- doesFileExist fp
    let msg :: T.Text = "Opted out of telemetry"
    optOut <- createLog gcp Lgr.Info msg
    unless exists do
        res <- sendLogs gcp [optOut]
        when (null res) $ writeFile fp ""

today :: IO Time.Day
today = Time.utctDay <$> getCurrentTime

-- | We decided 8MB a day is a fair max amount of data to send in telemetry
--   this was decided somewhat arbitrarily
maxDataPerDay :: SentBytes
maxDataPerDay = SentBytes (8 * 2 ^ (20 :: Int))

-- | The DAML home folder, getting this ensures the folder exists
getDamlDir :: IO FilePath
getDamlDir = do
    dh <- lookupEnv damlPathEnvVar
    dir <- case dh of
        Nothing -> fallback
        Just "" -> fallback
        Just var -> pure var
    liftIO $ createDirectoryIfMissing True dir
    pure dir
    where fallback = getAppUserDataDirectory "daml"

-- | Get the file for recording the sent data and acquire the corresponding lock.
withSentDataFile :: GCPState -> (FilePath -> IO a) -> IO a
withSentDataFile gcp@GCPState{gcpSentDataFileLock} f =
  withLock gcpSentDataFileLock $ do
      let fp = sentDataFile gcp
      exists <- doesFileExist fp
      let noSentData' = showSentData <$> noSentData
      unless exists (T.writeFileUtf8 fp =<< noSentData')
      f fp

data SendResult
    = ReachedDataLimit
    | HttpError SomeException
    | SendSuccess

isSuccess :: SendResult -> Bool
isSuccess SendSuccess = True
isSuccess _ = False

-- | This is parametrized over the actual send function to make it testable.
sendData :: GCPState -> (LBS.ByteString -> IO ()) -> LBS.ByteString -> IO SendResult
sendData gcp sendRequest payload = withSentDataFile gcp $ \sentDataFile -> do
    sentData <- validateSentData . parseSentData =<< T.readFileUtf8 sentDataFile
    let newSentData = addPayloadSize sentData
    T.writeFileUtf8 sentDataFile $ showSentData newSentData
    if sent newSentData >= maxDataPerDay
       then pure ReachedDataLimit
       else do
           r <- try $ sendRequest payload
           case r of
               Left e -> pure (HttpError e)
               Right _ -> pure SendSuccess
    where
        addPayloadSize :: SentData -> SentData
        addPayloadSize SentData{..} = SentData date (min maxDataPerDay (sent + payloadSize))
        payloadSize = SentBytes $ LBS.length payload

--------------------------------------------------------------------------------
-- TESTS -----------------------------------------------------------------------
--------------------------------------------------------------------------------
-- | These are not in the test suite because it hits an external endpoint
test :: IO ()
test = withGcpLogger (Lgr.Error ==) Lgr.Pure.makeNopHandle $ \hnd -> do
    let lg = Lgr.logError hnd
    let (ls :: [T.Text]) = replicate 13 $ "I like short songs!"
    liftIO do
        mapM_ lg ls
        putStrLn "Done!"
