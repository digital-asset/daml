-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DataLimit(main) where

import Control.Monad.Extra
import Test.Tasty
import Test.Tasty.HUnit
import DA.Service.Logger.Impl.GCP
import DA.Service.Logger.Impl.Pure
import System.Directory
import qualified Data.Text as T
import Data.Aeson (encode)
import System.Environment.Blank
import Data.Functor

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Telemetry data limiter"
    [ withResource homeDir (\mbHome -> whenJust mbHome unsetEnv) $ \getHomeDir ->
      withResource (initialiseGcpState (GCPConfig "test" Nothing) makeNopHandle) (\gcp -> removeFile (sentDataFile gcp)) $ \getGcp ->
          testCase "Check that limit is triggered" $ do
             _ <- getHomeDir
             gcp <- getGcp
             let d = replicate 100 $ encode $ T.pack $ replicate 100000 'X'
             res <- mapM (sendData gcp fakeSend) d
             let l = length $ takeWhile isSuccess res
             83 @=? l
  ]

fakeSend :: a -> IO ()
fakeSend _ = pure ()

-- | The CI env doesn't have a home directory so set and unset it if it doesn't exist
homeDir :: IO (Maybe String)
homeDir = do
    home <- getEnv "HOME"
    case home of
        Nothing -> fakeHome
        Just "" -> fakeHome
        Just _ -> pure Nothing

fakeHome :: IO (Maybe String)
fakeHome = setEnv "HOME" "." True $> Just "HOME"
