-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Telemetry(main) where

import Control.Monad.Extra
import DA.Service.Logger.Impl.GCP
import DA.Service.Logger.Impl.Pure
import DA.Test.Util
import Data.Aeson (encode)
import Data.Maybe
import qualified Data.Text as T
import System.Directory
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit

main :: IO ()
main =
  withTempDir $ \tmpDir0 -> do
  withTempDir $ \tmpDir1 -> do
  setPermissions tmpDir1 emptyPermissions
  withEnv [("TASTY_NUM_THREADS", Just "1")] $
    defaultMain $ do
      testGroup "Telemetry" [dataLimiterTests tmpDir0, cacheDirTests tmpDir1]

dataLimiterTests :: FilePath -> TestTree
dataLimiterTests cacheDir =
    testGroup
        "data limiter"
        [ withResource
                  (initialiseGcpState (GCPConfig "test" (Just cacheDir) Nothing) makeNopHandle)
                  (\gcpM -> whenJust gcpM $ \gcp -> removeFile (sentDataFile gcp)) $ \getGcp ->
                  testCase "Check that limit is triggered" $ do
                      gcpM <- getGcp
                      case gcpM of
                          Nothing -> fail "cache directory is not writable"
                          Just gcp -> do
                              let d = replicate 100 $ encode $ T.pack $ replicate 100000 'X'
                              res <- mapM (sendData gcp fakeSend) d
                              let l = length $ takeWhile isSuccess res
                              83 @=? l
        ]

cacheDirTests :: FilePath -> TestTree
cacheDirTests badCacheDir =
    testGroup
        "cache directory"
        [  withResource
                (initialiseGcpState (GCPConfig "test" (Just badCacheDir) Nothing) makeNopHandle)
                (\gcpM -> whenJust gcpM $ \gcp -> removeFile (sentDataFile gcp)) $ \getGcp ->
                testCase
                    "Check that a read-only cache dir is handled gracefully." $ do
                    gcpM <- getGcp
                    isNothing gcpM @=? True
        ]

fakeSend :: a -> IO ()
fakeSend _ = pure ()
