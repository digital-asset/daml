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
module DA.Service.Logger.Impl.GCP (
    gcpLogger
  , logOptOut
  -- * Test hooks
  , sendData
  , dfPath
  , test
  ) where

import GHC.Generics(Generic)
import Data.Int
import Data.Tuple
import Text.Read(readMaybe)
import Data.Aeson as Aeson
import Control.Monad
import GHC.Stack
import System.Directory
import System.Environment
import System.FilePath
import System.Info
import System.Timeout
import System.Random
import qualified DA.Service.Logger as Lgr
import qualified DA.Service.Logger.Impl.IO as Lgr.IO
import qualified DA.Service.Logger.Impl.Pure as Lgr.Pure
import DAML.Project.Consts
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import qualified Data.ByteString.UTF8 as UTFBS
import Data.Time as Time
import Data.UUID
import Control.Concurrent.Extra
import Control.Monad.Managed
import Control.Exception
import System.IO.Unsafe (unsafePerformIO)
import Control.Monad.Trans.State
import Control.Monad.Trans.Class
import Network.HTTP.Simple
import Network.HTTP.Types.Status


data GCPState = GCPState {
    _env :: Env
  , _logQueue :: Var [LogEntry]
  }

-- | Cached attempt to build an environment, on an exception try again next time
data Env = Env
    { envLogger :: Lgr.Handle IO
    , sessionID :: UUID
    }

fallBackLogger :: IO (Lgr.Handle IO)
fallBackLogger = Lgr.IO.newStderrLogger Lgr.Debug "Telemetry"

-- | This is where I'm going to put all the user data
initialiseEnv :: HasCallStack => IO Env
initialiseEnv = Env <$> fallBackLogger <*> randomIO

-- | retains the normal logging capacities of the handle but adds logging
--   to GCP
--   will attempt to flush the logs
gcpLogger ::
    HasCallStack =>
    (Lgr.Priority -> Bool) -- ^ if this is true log to GCP
    -> Lgr.Handle IO
    -> Managed (Lgr.Handle IO)
gcpLogger p hnd =
    fmap snd $ managed $ bracket
      initialiseState
      (\(gcp,_) -> flushLogsGCP gcp Lgr.Info logsFlushed)
  where

  initialiseState :: HasCallStack => IO (GCPState, Lgr.Handle IO)
  initialiseState = do
    env <- initialiseEnv
    logQueue <- newVar mempty
    let gcp = GCPState env logQueue
    let logger = hnd{Lgr.logJson = newLogJson gcp}
    -- this is here because it should be sent regardless of filtering
    logGCP gcp Lgr.Info =<< metaData
    pure (gcp, logger)

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

data LogEntry = LogEntry {
      severity :: !Lgr.Priority
    , timeStamp :: !UTCTime
    , message :: !(WithSession Value)
    }

instance ToJSON LogEntry where
    toJSON LogEntry{..} =
        Object $ HM.fromList [
          priorityToGCP severity
        , ("timestamp", toJSON timeStamp)
        , ("jsonPayload", toJSON message)
        ]

-- | this stores information about the users machine and is transmitted at the
--   start of every session
data MetaData = MetaData {
      machineID :: !UUID
    , operatingSystem :: !T.Text
    , version :: !(Maybe T.Text)
    } deriving Generic
instance ToJSON MetaData

metaData :: IO MetaData
metaData = do
    machineID <- fetchMachineID
    v <- lookupEnv sdkVersionEnvVar
    let version = case v of
            Nothing -> Nothing
            Just "" -> Nothing
            Just vs -> Just $ T.pack vs
    pure MetaData{
        machineID
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

createLog :: (Aeson.ToJSON a, HasCallStack)
          => Env -> Lgr.Priority -> a -> IO LogEntry
createLog env severity m = do
    let message = WithSession (sessionID env) $ toJSON m
    timeStamp <- getCurrentTime
    pure LogEntry{..}

-- | This will turn non objects into objects with a key of their type
--   e.g. 1e100 -> {"Number" : 1e100}, keys are "Array", "String", "Number"
--   "Bool" and "Null"
--   it will return objects unchanged
--   The payload must be a JSON object
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
priorityToGCP = (,) "severity". \case
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
    le <- createLog (_env gcp) priority js
    pushLogQueue gcp [le]
    void $ sendLogQueue gcp Sync

minBatchSize, maxBatchSize :: Int
minBatchSize = 10
maxBatchSize = 50

-- | Try to log something, if it doesn't work we won't tell you, but we'll try
--   again later, requests will be batched
logGCP
  :: Aeson.ToJSON a
  => GCPState
  -> Lgr.Priority
  -> a
  -> IO ()
logGCP gcp priority js = do
    le <- createLog (_env gcp) priority js
    pushLogQueue gcp [le]
    len <- logQueueLength gcp
    when (len >= minBatchSize) $
        sendLogQueue gcp Async

-- | try sending logs and return the ones that weren't sent
--   don't use this directly, instead use sendLogQueue
sendLogs :: [LogEntry] -> IO [LogEntry]
sendLogs contents =
  fmap getRemainder $ sendData send $ encode contents
    where
        send ::
            LBS.ByteString ->
            IO Bool -- ^ Was sent successfully
        send = fmap (statusIsSuccessful . getResponseStatus)
             . httpNoBody
             . toRequest
        getRemainder :: Maybe Bool -> [LogEntry]
        getRemainder = \case
            -- The limit has been reached so don't re-add anything
            Nothing -> []
            -- Sending failed so re-add
            Just False -> contents
            -- Sending succeeded so don't re-add
            Just True -> []

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
      defaultRequest

pushLogQueue ::
    GCPState ->
    [LogEntry] ->
    IO ()
pushLogQueue GCPState{..} logs =
    modifyVar_ _logQueue (pure . (logs ++))

popLogQueue ::
    GCPState ->
    IO [LogEntry]
popLogQueue GCPState{..} =
    modifyVar
    _logQueue
  $ pure . swap . splitAt maxBatchSize

logQueueLength ::
    GCPState ->
    IO Int
logQueueLength GCPState{..} =
    withVar _logQueue (pure . length)

data RunSync =
    Async
  | Sync

sendLogQueue ::
    GCPState ->
    RunSync ->
    IO ()
sendLogQueue gcp@GCPState{..} runSync = do
  let lgr = Lgr.logJson $ envLogger _env
      lgString = lgr Lgr.Error
  toSend <- popLogQueue gcp
  -- return True on success
  let handleResult = \case
          Left (err :: SomeException) -> do
              lgString $ displayException err
              pushLogQueue gcp toSend
          Right [] ->
              pure ()
          Right unsent -> do
              pushLogQueue gcp unsent
  let send = sendLogs toSend
  case runSync of
      Sync ->
          void $
          timeout 5_000_000 $
          handleResult =<< try send
      Async ->
          void $
          forkFinally send (handleResult)


--------------------------------------------------------------------------------
-- Prevent sending too much data -----------------------------------------------
--------------------------------------------------------------------------------
{-
This creates a file with todays date and the number of bytes of telemetry
data we have sent today, before sending it checks that this isn't in excess
of the maximum allowable amount of data. If the date doesn't match todays date
or the file is unreadable reset it to 0.
-}

data DataFile = DataFile {date :: Time.Day, sent :: DataSent}

type DataSent = Int64

-- | This machine ID is created once and read at every subsequent startup
--   it's a random number which is used to identify machines
fetchMachineID :: IO UUID
fetchMachineID = do
    fp <- fmap (</> ".machine_id") damlDir
    let generateID = do
        mID <- randomIO
        BS.writeFile fp $ encodeUtf8 $ toText mID
        pure mID
    exists <- doesFileExist fp
    if exists
       then do
        uid <- fromText . decodeUtf8 <$> BS.readFile fp
        maybe generateID pure uid
       else
        generateID

-- | If it hasn't already been done log that the user has opted out of telemetry
logOptOut :: IO ()
logOptOut = do
    fp <- fmap (</> ".opted_out") damlDir
    exists <- doesFileExist fp
    env <- initialiseEnv
    let msg :: T.Text = "Opted out of telemetry"
    optOut <- createLog env Lgr.Info msg
    unless exists do
        res <- sendLogs [optOut]
        when (Prelude.null res) $
            writeFile fp ""

-- | Reads the data file but doesn't check the values are valid
readDF :: UTFBS.ByteString -> Maybe DataFile
readDF s = do
  let t = UTFBS.toString s
  (date',sent') <-
    case lines t of
      [date, sent] -> Just (date, sent)
      _ -> Nothing
  date <- readMaybe date'
  sent <- readMaybe sent'
  pure DataFile{..}

-- | ensure the datatracker data is from today
validDF :: Maybe DataFile -> IO DataFile
validDF = \case
  Nothing -> emptyDF
  Just df -> do
    today' <- today
    if date df == today'
      then pure df
      else emptyDF

showDF :: DataFile -> UTFBS.ByteString
showDF DataFile{..} = UTFBS.fromString $ unlines [show date, show sent]

today :: IO Time.Day
today = Time.utctDay <$> getCurrentTime

-- | An empty DataFile with no data sent
emptyDF :: IO DataFile
emptyDF = DataFile <$> today <*> pure 0

-- | We decided 8MB a day is a fair max amount of data to send in telemetry
--   this was decided somewhat arbitrarily
maxDataPerDay :: DataSent -- ^ In bytes
maxDataPerDay = 8388608

{-# NOINLINE dataLock #-}
-- | acquire this lock if you're going to change data tracking file
dataLock :: Lock
dataLock = unsafePerformIO newLock

-- | The DAML home folder, getting this ensures the folder exists
damlDir :: IO FilePath
damlDir = do
    dh <- lookupEnv damlPathEnvVar
    dir <- case dh of
        Nothing -> fallback
        Just "" -> fallback
        Just var -> pure var
    liftIO $ createDirectoryIfMissing True dir
    pure dir
    where fallback = getAppUserDataDirectory "daml"

-- | Get the filename, this is locked until the managed monad is run
--   the file is guaranteed to exist
dfPath :: Managed FilePath
dfPath = do
  dir <- liftIO damlDir
  pth <- managed $ \action ->
    withLock dataLock $ action $ dir </> ".sent_data"
  exists <- liftIO $ doesFileExist pth
  let zeroDF = showDF <$> emptyDF
  unless exists
    $ liftIO $ BS.writeFile pth =<< zeroDF
  pure pth

overDataSent :: StateT DataSent IO r
             -> Managed r
overDataSent st = do
  pth <- dfPath
  contents <- liftIO $ BS.readFile pth
  df <- liftIO $ validDF $ readDF contents
  (ret, sentOut) <- liftIO $ runStateT st $ sent df
  liftIO $ BS.writeFile pth $ showDF df{sent=sentOut}
  pure ret



{- | This sends data up to the logs provided the data limit hasn't been hit
     This returns Nothing if the limit has been hit
-}
sendData :: forall a.
            (LBS.ByteString -> IO a)
         -> LBS.ByteString
         -> IO (Maybe a)
sendData f js = with (overDataSent wrk) pure where
  reqSize :: DataSent
  reqSize = LBS.length js
  wrk :: StateT DataSent IO (Maybe a)
  wrk = do
    sent <- get
    if
      | sent >= maxDataPerDay ->
        pure Nothing
      | sent + reqSize >= maxDataPerDay -> do
        {-
        To keep the logs simple, when you try and send a packet and that packet
        would have gone over the limit we increase the amount of data sent but
        do not actually send the data, this is to stop some small logs getting
        through when you're near the data cap
        -}
        modify' (reqSize+)
        pure Nothing
      | otherwise -> do
        modify' (reqSize+)
        Just <$> (lift $ f js)
--------------------------------------------------------------------------------
-- TESTS -----------------------------------------------------------------------
--------------------------------------------------------------------------------
-- | These are not in the test suite because it hits an external endpoint
test :: IO ()
test = runManaged do
    (hnd :: Lgr.Handle IO) <- gcpLogger (Lgr.Error ==) Lgr.Pure.makeNopHandle
    let lg = Lgr.logError hnd
    let (ls :: [T.Text]) = replicate 13 $ "I like short songs!"
    liftIO do
        mapM_ lg ls
        putStrLn "Done!"
