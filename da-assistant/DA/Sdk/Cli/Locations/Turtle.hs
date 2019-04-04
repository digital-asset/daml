-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Sdk.Cli.Locations.Turtle
    ( testfile
    , testdir
    , ls
    , mktree
    , cp
    , cptree
    , withTempDirectory
    , datefile

    , decodeFileEither
    , encodeFile
    ) where

import DA.Sdk.Prelude
import DA.Sdk.Cli.Locations.Types
import qualified Turtle as T
import qualified System.IO.Temp as Tmp
import Control.Monad.Catch (MonadMask)
import qualified Control.Foldl as Foldl
import qualified Data.Yaml as Yaml

testfile :: (IsFilePath t, MonadIO m) => FilePathOf t -> m Bool
testfile f = liftIO $ T.testfile $ unwrapFilePathOf f

testdir :: (IsDirPath t, MonadIO m) => FilePathOf t -> m Bool
testdir d = liftIO $ T.testdir $ unwrapFilePathOf d

mktree :: (IsDirPath t, MonadIO m) => FilePathOf t -> m ()
mktree f = liftIO $ T.mktree $ unwrapFilePathOf f

cptree :: MonadIO m => T.FilePath -> FilePathOf t -> m ()
cptree from to = liftIO $ T.cptree from (unwrapFilePathOf to) 

cp :: MonadIO m => T.FilePath -> FilePathOf t -> m ()
cp from to = liftIO $ T.cp from (unwrapFilePathOf to) 

ls :: MonadIO m => FilePathOf a -> m [T.FilePath]
ls path = T.fold (T.ls . unwrapFilePathOf $ path) Foldl.list

datefile :: MonadIO m => FilePathOf a -> m T.UTCTime
datefile = T.datefile . unwrapFilePathOf

withTempDirectory :: (IsDirPath t, MonadMask m, MonadIO m) => FilePathOf t -> String -> (String -> m a) -> m a
withTempDirectory tempDir =
    Tmp.withTempDirectory $ pathToString $ unwrapFilePathOf tempDir

decodeFileEither :: Yaml.FromJSON b => FilePathOf a -> IO (Either Yaml.ParseException b)
decodeFileEither = Yaml.decodeFileEither . pathToString . unwrapFilePathOf

encodeFile :: Yaml.ToJSON b => FilePathOf a -> b -> IO ()
encodeFile f e = Yaml.encodeFile (pathToString $ unwrapFilePathOf f) e