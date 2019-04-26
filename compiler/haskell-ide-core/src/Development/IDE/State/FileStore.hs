-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TypeFamilies #-}

module Development.IDE.State.FileStore(
    getFileExists, getFileContents,
    setBufferModified,
    fileStoreRules
    ) where



import           "ghc-lib-parser" StringBuffer

import qualified Data.Map.Strict as Map
import qualified Data.Text as T
import           Data.Time.Clock
import           Control.Monad.Extra
import qualified System.Directory as Dir
import           Development.Shake
import           Development.Shake.Classes
import           Development.IDE.State.Shake
import Development.IDE.UtilGHC
import           Control.Concurrent.Extra
import           Control.Exception
import           GHC.Generics
import qualified Data.ByteString.Char8 as BS
import Development.IDE.Types.Diagnostics
import           Data.Time


-- This module stores the changed files in memory, and answers file system questions
-- from either the memory changes OR the file system itself

type DirtyFiles = Map.Map FilePath (UTCTime, StringBuffer) -- when it was modified, it's current value

-- Store the DirtyFiles globally, so we can get at it through setBufferModified
newtype GlobalDirtyFiles = GlobalDirtyFiles (Var DirtyFiles)
instance IsIdeGlobal GlobalDirtyFiles



-- | Get the modification time of a file.
type instance RuleResult GetModificationTime = UTCTime

-- | Get the contents of a file, either dirty (if the buffer is modified) or from disk.
type instance RuleResult GetFileContents = (UTCTime, StringBuffer)

-- | Does the file exist.
type instance RuleResult GetFileExists = Bool


data GetFileExists = GetFileExists
    deriving (Eq, Show, Generic)
instance Binary   GetFileExists
instance Hashable GetFileExists
instance NFData   GetFileExists

data GetModificationTime = GetModificationTime
    deriving (Eq, Show, Generic)
instance Binary   GetModificationTime
instance Hashable GetModificationTime
instance NFData   GetModificationTime

data GetFileContents = GetFileContents
    deriving (Eq, Show, Generic)
instance Binary   GetFileContents
instance Hashable GetFileContents
instance NFData   GetFileContents


getFileExistsRule :: Var DirtyFiles -> Rules ()
getFileExistsRule dirty =
    defineEarlyCutoff $ \GetFileExists file -> do
        alwaysRerun
        res <- liftIO $ handle (\(_ :: IOException) -> return False) $
            (Map.member file <$> readVar dirty) ||^
            Dir.doesFileExist file
        return (Just $ if res then BS.singleton '1' else BS.empty, ([], Just res))


showTimePrecise :: UTCTime -> String
showTimePrecise UTCTime{..} = show (toModifiedJulianDay utctDay, diffTimeToPicoseconds utctDayTime)

getModificationTimeRule :: Var DirtyFiles -> Rules ()
getModificationTimeRule dirty =
    defineEarlyCutoff $ \GetModificationTime file -> do
        alwaysRerun
        res <- liftIO $ ideTryIOException file $ do
            mp <- readVar dirty
            case Map.lookup file mp of
                Just (time, _) -> return time
                Nothing -> Dir.getModificationTime file
        case res of
            Left err -> return (Nothing, ([err], Nothing))
            Right time -> return (Just $ BS.pack $ showTimePrecise time, ([], Just time))


getFileContentsRule :: Var DirtyFiles -> Rules ()
getFileContentsRule dirty =
    define $ \GetFileContents file -> do
        -- need to depend on modification time to introduce a dependency with Cutoff
        time <- use_ GetModificationTime file
        res <- liftIO $ ideTryIOException file $ do
            mp <- readVar dirty
            case Map.lookup file mp of
                Just (_, contents) -> return contents
                Nothing -> hGetStringBuffer file
        case res of
            Left err -> return ([err], Nothing)
            Right contents -> return ([], Just (time, contents))


getFileContents :: FilePath -> Action (UTCTime, StringBuffer)
getFileContents = use_ GetFileContents

getFileExists :: FilePath -> Action Bool
getFileExists =
    -- we deliberately and intentionally wrap the file as an FilePath WITHOUT mkAbsolute
    -- so that if the file doesn't exist, is on a shared drive that is unmounted etc we get a properly
    -- cached 'No' rather than an exception in the wrong place
    use_ GetFileExists


fileStoreRules :: Rules ()
fileStoreRules = do
    dirty <- liftIO $ newVar Map.empty
    addIdeGlobal $ GlobalDirtyFiles dirty
    getModificationTimeRule dirty
    getFileContentsRule dirty
    getFileExistsRule dirty


strictPair :: a -> b -> (a, b)
strictPair !a !b = (a,b)


-- | Notify the compiler service of a modified buffer
setBufferModified :: IdeState -> FilePath -> (Maybe T.Text, UTCTime) -> IO ()
setBufferModified state absFile (mcontents, !time) = do
    GlobalDirtyFiles envDirtyFiles <- getIdeGlobalState state
    -- update vars synchronously
    modifyVar_ envDirtyFiles $ evaluate . case mcontents of
        Nothing -> Map.delete absFile
        Just contents -> Map.insert absFile $ strictPair time (textToStringBuffer contents)

    -- run shake to update results regarding the files of interest
    void $ shakeRun state []
