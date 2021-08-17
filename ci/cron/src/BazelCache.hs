-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | This module is a workaround for the `output not created` error we
-- see on Windows CI.  We iterate over all AC entries in the cache and
-- look for broken entries with no output and delete those.  This
-- fixes the build for nodes that have only fetched this from the
-- cache.  For other nodes, it looks like a `clean --expunge` is also
-- required (or a full node reset).  See
-- https://github.com/tweag/rules_haskell/issues/1260 for more
-- information.

module BazelCache
    ( Opts(..)
    , Delete(..)
    , run
    ) where

import Build.Bazel.Remote.Execution.V2.RemoteExecution (ActionResult(..), Digest(..), OutputDirectory(..))
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Concurrent.STM.TBMQueue
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource
import qualified Data.ByteString.Lazy as BSL
import Data.Conduit ((.|))
import qualified Data.Conduit as Conduit
import qualified Data.Conduit.Combinators as Conduit
import qualified Data.Conduit.Process.Typed as Conduit
import qualified Data.Conduit.Text as Conduit
import qualified Data.Conduit.TQueue as Conduit
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time
import Data.Time.Format.ISO8601
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import qualified Proto3.Suite as Proto3
import System.IO

data Opts = Opts
  { age :: NominalDiffTime
  -- ^ Maximum age of entries that will be considered.
  , cacheSuffix :: Maybe String
  -- ^ Optional cache suffix to limit the search to.
  , queueSize :: Int
  -- ^ Size of the queue used to distribute work.
  , concurrency :: Int
  -- ^ Number of concurrent workers.
  , delete :: Delete
  -- ^ Whether invalid entries should be deleted.
  }

newtype Delete = Delete Bool

run :: Opts -> IO ()
run Opts{..} = do
    now <- getCurrentTime
    let oldest = addUTCTime (- age) now
    let procSpec =
            Conduit.setStdout Conduit.createSource $
            Conduit.proc "gsutil" ["list", "-l", gsCachePath cacheSuffix]
    manager <- newManager tlsManagerSettings
    runResourceT $ do
        (reg, queue) <- allocate (newTBMQueueIO queueSize) (atomically . closeTBMQueue)
        workers <- replicateM concurrency $ do
            (_, worker) <- allocate (async (worker delete manager queue)) uninterruptibleCancel
            pure worker
        liftIO $ Conduit.withProcessWait procSpec $ \p -> do
            let outConduit = Conduit.getStdout p
            Conduit.runConduit
              $ outConduit
              .| Conduit.decode Conduit.utf8 .| Conduit.lines
              .| Conduit.filter (not . isTotal)
              .| Conduit.concatMapM (filterLine oldest)
              .| Conduit.sinkTBMQueue queue
        release reg
        liftIO $ mapM_ wait workers

worker :: Delete -> Manager -> TBMQueue (UTCTime, Text) -> IO ()
worker delete manager queue = Conduit.runConduit
    $ Conduit.sourceTBMQueue queue
    .| Conduit.concatMapM (validateArtifact manager)
    .| Conduit.mapM_ (handleInvalid delete)

-- | Handle an invalid entry.
handleInvalid :: Delete -> (UTCTime, Text, ActionResult) -> IO ()
handleInvalid (Delete delete) (time, path, r) = do
    putStrLn $ "Found invalid AC at " <> show path <> " created at " <> show time <> ": " <> show r
    when delete $ do
        putStrLn $ "Deleting AC " <> show path
        exit <- Conduit.runProcess $
            Conduit.proc "gsutil" ["rm", "gs://daml-bazel-cache/" <> T.unpack path]
        putStrLn $ "Exit code: " <> show exit

-- | Filter to lines that parse and are for entries that are not older
-- than the supplied age.
filterLine :: UTCTime -> T.Text -> IO (Maybe (UTCTime, T.Text))
filterLine oldest s = case parseLine s of
    Nothing -> do
        hPutStrLn stderr $
            "ERROR: failed to parse " <> show s <> ", ignoring"
        pure Nothing
    Just (time, entry)
        | time >= oldest -> pure (Just (time, entry))
        | otherwise -> pure Nothing

-- | Download and validate the AC artifact at the given path.
-- Returns Nothing for valid artifacts and Just _ for a broken
-- arfiact.
validateArtifact :: Manager -> (UTCTime, Text) -> IO (Maybe (UTCTime, Text, ActionResult))
validateArtifact manager (time, path) = do
    req <- parseUrlThrow (cacheUrl (T.unpack path))
    resp <- httpLbs req manager
    let bs = responseBody resp
    case Proto3.fromByteString (BSL.toStrict bs) of
      Left err -> do
        hPutStrLn stderr $ concat
            [ "ERROR: malformed AC entry at"
            , show path
            , ":"
            , show err
            , ", ignoring"
            ]
        pure Nothing
      Right ac
          | isInvalid ac -> pure (Just (time, path, ac))
          | otherwise -> pure Nothing

  where
    isInvalid ActionResult{..} = and
      [ null actionResultOutputFiles
      , all (\dir -> maybe True (\Digest{..} -> digestSizeBytes == 0 || digestHash == brokenDigestHash) (outputDirectoryTreeDigest dir)) actionResultOutputDirectories
      , maybe True (\r -> digestSizeBytes r == 0) actionResultStdoutDigest
      , maybe True (\r -> digestSizeBytes r == 0) actionResultStderrDigest
      ]
    -- This corresponds to 0x0a 0x00 which is a protobuf message for the digest
    -- with en empty string.
    brokenDigestHash = "102b51b9765a56a3e899f7cf0ee38e5251f9c503b357b330a49183eb7b155604"

-- | Checks for the last line in `gsutil -l`’s output.
isTotal :: Text -> Bool
isTotal = T.isPrefixOf "TOTAL: "

-- | Parse a single line in the output of `gsutil -l`
-- into the time and the cache path.
parseLine :: Text -> Maybe (UTCTime, Text)
parseLine t = do
    [_, timeStr, name] <- pure (T.words t)
    time <- iso8601ParseM (T.unpack timeStr)
    path <- T.stripPrefix "gs://daml-bazel-cache/" name
    pure (time, path)

gsCachePath :: Maybe String -> String
gsCachePath mbSuffix = "gs://daml-bazel-cache/" <> suffix <> "/ac/*"
  -- Filtering to *-v* isn’t strictly necessary but it ensures that
  -- we do not walk through the Linux cache which seems to
  -- speed things up a bit.
  where suffix = fromMaybe "*-v*" mbSuffix

cacheUrl :: String -> String
cacheUrl path = "https://bazel-cache.da-ext.net/" <> path

