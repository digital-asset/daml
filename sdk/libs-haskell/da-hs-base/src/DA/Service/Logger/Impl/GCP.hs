-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
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
    , GCPConfig(..)
    , GCPState(..)
    , initialiseGcpState
    , setOptIn
    , isOptedIn
    , isOptedOut
    , logOptOut
    , logIgnored
    , logMetaData
    , SendResult(..)
    , isSuccess
    , sendData
    , sentDataFile
    -- * Test hooks
    , test
    ) where

import GHC.Generics(Generic)
import Data.Int
import Text.Read (readMaybe)
import Data.Aeson as Aeson
import qualified Data.Aeson.KeyMap as Aeson
import Data.Char (toUpper)
import Control.Monad
import Control.Monad.Loops
import GHC.Stack
import System.Directory
import System.Environment
import System.FilePath
import System.Info
import System.Timeout
import System.Random
import System.IO.Extra
import qualified DA.Service.Logger as Lgr
import qualified DA.Service.Logger.Impl.Pure as Lgr.Pure
import DA.Daml.Project.Consts
import qualified Data.Text.Extended as T
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import Data.String
import Data.Time as Time
import Data.UUID (UUID)
import qualified Data.UUID as UUID
import Control.Concurrent.Async
import Control.Concurrent.Extra
import Control.Concurrent.STM
import Control.Exception.Safe
import Network.HTTP.Simple
import SdkVersion

-- Type definitions

-- | Tag used to distinguish between different components that use the
-- GCP logger, such as "daml" or "ide". This should be a valid suffix
-- for a file name.
newtype GCPTag = GCPTag { unGCPTag :: String }
    deriving (IsString)

-- | Configuration for setting up GCP logger.
data GCPConfig = GCPConfig
    { gcpConfigTag :: GCPTag
    , gcpConfigCachePath :: Maybe FilePath
    , gcpConfigDamlPath :: Maybe FilePath
    }

data GCPState = GCPState
    { gcpFallbackLogger :: Lgr.Handle IO
    -- ^ Fallback logger to log exceptions caused by the GCP logging itself.
    , gcpLogChan :: TChan (LogEntry, IO ())
    -- ^ Unsent logs. The IO action is a finalizer that is run when the log entry
    -- has been sent successfully.
    , gcpSessionID :: UUID
    -- ^ Identifier for the current session
    , gcpCacheDir :: FilePath
    -- ^ Directory where we store various files such as the amount of
    -- data sent so far.
    , gcpSentDataFileLock :: Lock
    -- ^ Lock for accessing sendData.
    -- Note that this is not safe if there are multiple damlc executables
    -- running. However, we can handle a corrupted data file gracefully
    -- and cross-platform file locking is annoying so we do not bother
    -- with a lock that works across processes.
    , gcpTag :: GCPTag
    -- ^ A tag to keep different SDK components that use the GCP logger from
    -- interfering with each other. This is all so we don't have to deal with
    -- cross-platform file locking.
    }

newtype SentBytes = SentBytes { getSentBytes :: Int64 }
    deriving (Eq, Ord, Num)

data SentData = SentData
    { date :: Time.Day
    , sent :: SentBytes
    }

-- Parameters

-- | Timeout on log requests
requestTimeout :: Int
requestTimeout = 5_000_000

-- Files used to record data that should persist over restarts.

sentDataFile :: GCPState -> FilePath
sentDataFile GCPState{..} =
    gcpCacheDir </> (".sent_data_" <> unGCPTag gcpTag)

machineIDFile :: GCPState -> FilePath
machineIDFile GCPState{gcpCacheDir} = gcpCacheDir </> ".machine_id"

optedOutFile :: GCPState -> FilePath
optedOutFile GCPState{gcpCacheDir} = gcpCacheDir </> ".opted_out"

optedInFile :: GCPState -> FilePath
optedInFile GCPState{gcpCacheDir} = gcpCacheDir </> ".opted_in"

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

initialiseGcpState :: GCPConfig -> Lgr.Handle IO -> IO (Maybe GCPState)
initialiseGcpState GCPConfig {..} gcpFallbackLogger = do
    gcpCacheDirM <- getCacheDir gcpConfigCachePath
    -- copy machine_id file to cache directory to preserve statistics.
    let damlPathAndCachePathM = do
            cachePath <- gcpCacheDirM
            damlPath <- gcpConfigDamlPath
            Just (cachePath, damlPath)
    forM_ damlPathAndCachePathM $ \(cachePath, damlPath) -> do
        let machineIdPath = damlPath </> ".machine_id"
        let newMachineIdPath = cachePath </> ".machine_id"
        hasMachineId <- doesFileExist machineIdPath
        hasNewMachineId <- doesFileExist newMachineIdPath
        when (hasMachineId && not hasNewMachineId) $ do
          copyFile machineIdPath newMachineIdPath
          removeFile machineIdPath
    gcpLogChan <- newTChanIO
    gcpSessionID <- randomIO
    gcpSentDataFileLock <- newLock
    let gcpTag = gcpConfigTag
    pure $ fmap (\gcpCacheDir -> GCPState {..}) gcpCacheDirM

-- | Read everything from the chan that you can within a single transaction.
drainChan :: TChan a -> IO [a]
drainChan chan = atomically $ unfoldM (tryReadTChan chan)

-- | retains the normal logging capacities of the handle but adds logging
--   to GCP
--   will attempt to flush the logs
withGcpLogger
    :: GCPConfig
    -> (Lgr.Priority -> Bool) -- ^ if this is true log to GCP
    -> Lgr.Handle IO
    -> (Maybe GCPState -> Lgr.Handle IO -> IO a)
    -- ^ We give access to both the GCPState as well as the modified logger
    -- since the GCPState can be useful to bypass the message filter, e.g.,
    -- for metadata messages which have info prio.
    -> IO a
withGcpLogger config p hnd f = do
    gcpStateM <- initialiseGcpState config hnd
    case gcpStateM of
      Just gcpState -> do
            let logger = hnd
                    { Lgr.logJson = newLogJson gcpState
                    }
            let worker = forever $ mask_ $ do
                    -- We mask to avoid messages getting lost.
                    entry <- atomically (readTChan (gcpLogChan gcpState))
                    sendLogs gcpState [entry]
            (withAsync worker $ \_ -> do
                f (Just gcpState) logger) `finally` do
                logs <- drainChan (gcpLogChan gcpState)
                sendLogs gcpState logs
                Lgr.logJson hnd Lgr.Info ("Flushed " <> show (length logs) <> " logs")
      Nothing -> f Nothing hnd
  where
      newLogJson ::
          HasCallStack =>
          Aeson.ToJSON a =>
          GCPState -> Lgr.Priority -> a -> IO ()
      newLogJson gcp priority js = do
          Lgr.logJson hnd priority js
          when (p priority) $
              logGCP gcp priority js (pure ())

data LogEntry = LogEntry
    { severity :: !Lgr.Priority
    , timeStamp :: !UTCTime
    , message :: !(WithSession Value)
    }

instance ToJSON LogEntry where
    toJSON LogEntry{..} = Object $ Aeson.fromList
        [ priorityToGCP severity
        , ("timestamp", toJSON timeStamp)
        , ("jsonPayload", toJSON message)
        ]

-- | this stores information about the users machine and is transmitted at the
--   start of every session
data MetaData = MetaData
    { machineID :: !UUID
    , operatingSystem :: !T.Text
    , version :: !T.Text
    } deriving Generic
instance ToJSON MetaData

logMetaData :: GCPState -> IO ()
logMetaData gcpState = do
    metadata <- getMetaData gcpState
    logGCP gcpState Lgr.Info metadata (pure ())

getMetaData :: GCPState -> IO MetaData
getMetaData gcp = do
    machineID <- fetchMachineID gcp
    v <- lookupEnv sdkVersionEnvVar
    let version = T.pack $ case v of
            Just vs | not (null vs) -> vs
            -- If damlc is invoked directly which people might do when using other LSP clients, e.g.,
            -- vim we cannot rely on the version being set by the assistant.
            -- Therefore we fall back on the SDK version at built time which
            -- should be correct with the exception of daml-sdk-head where it will not be set to 0.0.0.
            _ -> sdkVersion
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
            Aeson.insert "SESSION_ID" (toJSON wsID) $
            toJsonObject $
            toJSON wsContents

toLogEntry :: Aeson.ToJSON a => GCPState -> Lgr.Priority -> a -> IO LogEntry
toLogEntry GCPState{gcpSessionID} severity m = do
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
             -> Aeson.KeyMap Value
toJsonObject v = either (\k -> Aeson.singleton k v) id $ objectOrKey v where
  objectOrKey = \case
    Aeson.Object o -> Right o
    Aeson.Array _ -> Left "Array"
    Aeson.String _ -> Left "String"
    Aeson.Number _ -> Left "Number"
    Aeson.Bool _ -> Left "Bool"
    Aeson.Null -> Left "Null"

-- | Map from our custom severities to Google Cloud Stackdriver's LogSeverity.
-- Most severity levels are named the same, though we map Telemetry to NOTICE.
-- This is necessary as a log message is rejected if its severity is not valid.
priorityToGCP :: Lgr.Priority -> (Key, Value)
priorityToGCP prio = ("severity", prio')
    where
      prio' = case prio of
        Lgr.Telemetry -> "NOTICE"
        _ -> toJSON $ map toUpper $ show prio

-- | Add something to the log queue.
logGCP
    :: Aeson.ToJSON a
    => GCPState
    -> Lgr.Priority
    -> a
    -> IO ()
    -> IO ()
logGCP gcp@GCPState{gcpLogChan} priority js finalizer = do
    le <- toLogEntry gcp priority js
    atomically $ writeTChan gcpLogChan (le, finalizer)

-- | Try sending logs. If anything fails, the logs are simply discarded.
-- sendLogs also takes care of adding a timeout.
sendLogs :: GCPState -> [(LogEntry, IO ())] -> IO ()
sendLogs gcp (unzip -> (entries, finalizers)) = unless (null entries) $ do
    res <- timeout requestTimeout $ sendData gcp (void . httpNoBody . toRequest) $ encode entries
    case res of
        Nothing -> Lgr.logJson (gcpFallbackLogger gcp) Lgr.Info ("Timeout while sending log request" :: T.Text)
        Just (HttpError e) -> logException gcp e
        Just ReachedDataLimit -> pure ()
        Just SendSuccess -> mapM_ tryAny finalizers

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

-- | Write out a `.opted_in` file to the daml home directory, if it doesn't exist,
-- and remove any existing `.opted_out`.
setOptIn :: GCPState -> IO ()
setOptIn gcp = do
    let fpOut = optedOutFile gcp
        fpIn = optedInFile gcp
    writeFile fpIn ""
    removePathForcibly fpOut

isOptedIn :: FilePath -> IO Bool
isOptedIn cache = doesPathExist $ cache </> ".opted_in"

isOptedOut :: FilePath -> IO Bool
isOptedOut cache = doesPathExist $ cache </> ".opted_out"

-- | If it hasn't already been done log that the user has opted out of telemetry.
logOptOut :: GCPState -> IO ()
logOptOut gcp = do
    let fpOut = optedOutFile gcp
        fpIn = optedInFile gcp
    removePathForcibly fpIn
    exists <- doesFileExist fpOut
    metadata <- getMetaData gcp
    let val = disabledMessage metadata "Opted out of telemetry"
    unless exists do
        logGCP gcp Lgr.Info val $ do
            writeFile fpOut ""

-- | Turn a message describing why telemetry was disabled (opt-out or no choice made) into an Aeson value
-- that includes the machine id.
disabledMessage :: MetaData -> T.Text -> Aeson.Value
disabledMessage metadata msg =
    -- The slightly odd format here is to make sure that we don’t break old queries before machineID was part of opt-out messages.
    toJSON $ Aeson.insert "machineID" (toJSON $ machineID metadata) $ toJsonObject (toJSON msg)

-- Log that the user clicked away the telemetry popup without making a choice.
logIgnored :: GCPState -> IO ()
logIgnored gcp = do
    metadata <- getMetaData gcp
    let val = disabledMessage metadata "No telemetry choice"
    logGCP gcp Lgr.Info val (pure ())

today :: IO Time.Day
today = Time.utctDay <$> getCurrentTime

-- | We decided 8MB a day is a fair max amount of data to send in telemetry
--   this was decided somewhat arbitrarily
maxDataPerDay :: SentBytes
maxDataPerDay = SentBytes (8 * 2 ^ (20 :: Int))

-- | Get the cache folder. Returns Nothing if the cache folder is not writable or not available.
getCacheDir :: Maybe FilePath -> IO (Maybe FilePath)
getCacheDir cachePathM =
    case cachePathM of
        Nothing -> pure Nothing
        Just cachePath -> do
            errOrVoid <- tryIO $ createDirectoryIfMissing True cachePath
            case errOrVoid of
                Left _err -> pure Nothing
                Right () -> do
                    perms <- getPermissions cachePath
                    pure $
                        if writable perms
                            then Just cachePath
                            else Nothing

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
test = withTempDir $ \tmpDir -> withGcpLogger (GCPConfig "test" (Just tmpDir) Nothing) (Lgr.Error ==) Lgr.Pure.makeNopHandle $ \_gcp hnd -> do
    let lg = Lgr.logError hnd
    let (ls :: [T.Text]) = replicate 13 "I like short songs!"
    mapM_ lg ls
    putStrLn "Done!"
