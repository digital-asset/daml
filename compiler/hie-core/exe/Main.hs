-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import Arguments
import Data.Maybe
import Control.Concurrent.Extra
import Control.Monad
import Data.Default
import System.Time.Extra
import Development.IDE.Core.FileStore
import Development.IDE.Core.OfInterest
import Development.IDE.Core.Service
import Development.IDE.Core.Rules
import Development.IDE.Core.Shake
import Development.IDE.Core.RuleTypes
import Development.IDE.LSP.Protocol
import Development.IDE.Types.Location
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Options
import Development.IDE.Types.Logger
import qualified Data.Text.IO as T
import Language.Haskell.LSP.Messages
import Development.IDE.LSP.LanguageServer
import System.Directory
import System.Environment
import System.IO
import Development.Shake hiding (Env)
import qualified Data.Set as Set

-- import CmdLineParser
-- import DynFlags
-- import Panic
import GHC
import qualified GHC.Paths

import HIE.Bios

-- Set the GHC libdir to the nix libdir if it's present.
getLibdir :: IO FilePath
getLibdir = fromMaybe GHC.Paths.libdir <$> lookupEnv "NIX_GHC_LIBDIR"

main :: IO ()
main = do
    -- WARNING: If you write to stdout before runLanguageServer
    --          then the language server will not work
    hPutStrLn stderr "Starting hie-core"
    Arguments{..} <- getArguments

    -- lock to avoid overlapping output on stdout
    lock <- newLock
    let logger = makeOneLogger $ withLock lock . T.putStrLn

    dir <- getCurrentDirectory
    hPutStrLn stderr dir

    if argLSP then do
        t <- offsetTime
        hPutStrLn stderr "Starting LSP server..."
        runLanguageServer def def $ \event vfs -> do
            t <- t
            hPutStrLn stderr $ "Started LSP server in " ++ showDuration t
            let options = defaultIdeOptions $ liftIO $ newSession' =<< findCradle (dir <> "/")
            initialise (mainRule >> action kick) event logger options vfs
    else do
        putStrLn "[1/5] Finding hie-bios cradle"
        cradle <- findCradle (dir <> "/")
        print cradle

        putStrLn "[2/5] Converting Cradle to GHC session"
        env <- newSession' cradle

        putStrLn "[3/3] Running sessions"
        let files = map toNormalizedFilePath argFiles
        vfs <- makeVFSHandle
        ide <- initialise mainRule (showEvent lock) logger (defaultIdeOptions $ return env) vfs
        setFilesOfInterest ide $ Set.fromList files
        runAction ide kick
        -- shake now writes an async message that it is completed with timing info,
        -- so we sleep briefly to wait for it to have been written
        sleep 0.01
        putStrLn "Done"


kick :: Action ()
kick = do
    files <- getFilesOfInterest
    void $ uses TypeCheck $ Set.toList files

-- | Print an LSP event.
showEvent :: Lock -> FromServerMessage -> IO ()
showEvent _ (EventFileDiagnostics _ []) = return ()
showEvent lock (EventFileDiagnostics (toNormalizedFilePath -> file) diags) =
    withLock lock $ T.putStrLn $ showDiagnosticsColored $ map (file,) diags
showEvent lock e = withLock lock $ print e

newSession' :: Cradle -> IO HscEnv
newSession' cradle = getLibdir >>= \libdir -> runGhc (Just libdir) $ do
    initializeFlagsWithCradle "" cradle
    getSession
