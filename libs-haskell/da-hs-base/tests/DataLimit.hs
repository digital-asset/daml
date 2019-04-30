-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DataLimit(main) where

import Test.Tasty
import Test.Tasty.HUnit
import DA.Service.Logger.Impl.GCP
import Control.Monad.Managed
import Data.Maybe
import System.Directory
import qualified Data.Text as T
import Data.Aeson (encode)
import System.Environment
import Data.Functor

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Telemetry data limiter" [
  withCleanup $
    testCase "Check that limit is triggered" $ do
      let d = replicate 100 $ encode $ T.pack $ replicate 100000 'X'
      res <- mapM (sendData fakeSend) d
      let l = length $ takeWhile isJust res
      83 @=? l
  ]

withCleanup :: TestTree -> TestTree
withCleanup t = withResource
                withHomeDir
                cleanUp
                (const t)

fakeSend :: a -> IO ()
fakeSend _ = pure ()

cleanUp :: Maybe String -> IO ()
cleanUp s = runManaged $ do
  pth <- dfPath
  liftIO $ removeFile pth
  case s of
      Just x -> liftIO $ unsetEnv x
      Nothing -> pure ()

-- | The CI env doesn't have a home directory so set and unset it if it doesn't exist
withHomeDir :: IO (Maybe String)
withHomeDir = do
    home <- lookupEnv "HOME"
    case home of
        Nothing -> fakeHome
        Just "" -> fakeHome
        Just _ -> pure Nothing

fakeHome :: IO (Maybe String)
fakeHome = setEnv "HOME" "." $> Just "HOME"
