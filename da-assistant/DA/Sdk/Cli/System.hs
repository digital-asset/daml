-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module DA.Sdk.Cli.System
    ( startNewProcess
    , executeFile
    , getTermWidth
    , setFileMode
    , runAsDaemonProcess
    , runShellScript
    , runInstaller

    , linkOrCopy
    , linkOrCopy'
    , removeDaHomeDir

    , PC.unionFileModes
    , PC.ownerReadMode
    , PC.ownerWriteMode
    , FileMode

    , daDir
    , daRelativeBinDir
    , daRelativeBinName
    , installationDir
    , exitFailure

    , createBinaryStarter
    , allBinaryExtensions
    , osSpecificBinaryExtensions
    , unixBinaryExtensions
    , windowsBinaryExtensions

    , unsupportedOnWin

    , isSudo
    ) where

import System.Process                          (callProcess, Pid, readCreateProcessWithExitCode, proc)
import Control.Monad                           (void)
import qualified System.Console.Terminal.Size  as Terminal.Size
import           GHC.IO.Handle                 (hIsTerminalDevice)
import           System.Directory              as D
import           System.IO                     (stdout)
import qualified System.PosixCompat.Files      as PC
import           System.Posix.Types            (FileMode)
import           System.Exit                   (exitFailure)
import           Control.Exception             (SomeException, catch, try, IOException)
import qualified Turtle                        as T
import           DA.Sdk.Cli.Locations.Types    (FilePathOfDaHomeDir, FilePathOfDaBinary, unwrapFilePathOf, pathOfToString)
import qualified DA.Sdk.Cli.Locations.Types    as L
import qualified DA.Sdk.Cli.Locations.Turtle   as LT
import           DA.Sdk.Prelude                hiding (FilePath)
import           Control.Monad.Logger          hiding (logWarn)
import           Data.Text                     (pack)
import           System.Environment

-- ----------------------------------------------------------------------------
-- System dependent imports and foreign calls:
-- ----------------------------------------------------------------------------

-- This is enough for Windows detection, see: https://guide.aelve.com/haskell/cpp-vww0qd72
#if !defined(mingw32_HOST_OS)
-- Posix only
import qualified System.Posix.Files            as PIO
import qualified System.Posix.IO               as PIO
import           System.Posix.Process          (createSession, forkProcess)
import qualified System.Posix.Process          as PP
#else
-- Windows only
import System.Win32.Types hiding (try)
import Graphics.Win32.GDI.Types
import Foreign.C.String
import Foreign.Marshal.Array
import Data.Time.Clock
import Data.Time.Format
import System.Process (CreateProcess(..))
import Control.Monad.Extra (whenM)
import Control.Monad.Logger (logWarnN)
import System.Process (callProcess, createProcess, delegate_ctlc, env
                      , proc, withCreateProcess, waitForProcess, Pid)
import System.Exit (exitSuccess, ExitCode(..))

foreign import ccall unsafe "SHGetFolderPathW"
    cSHGetFolderPathW :: HWND -> INT -> HANDLE -> DWORD -> CWString -> IO LONG

maxPath = 260
cSIDL_LOCAL_APPDATA = 0x001c -- see file ShlObj.h in MS Platform SDK for other CSIDL constants

-- Get a Windows "Shell folder" like local app data
getShellFolder :: MonadIO m => INT -> m T.FilePath
getShellFolder csidl = liftIO $ allocaArray0 maxPath $ \path -> do
    cSHGetFolderPathW nullHANDLE csidl nullHANDLE 0 path
    stringToPath <$> peekCWString path
#endif

-- ----------------------------------------------------------------------------
-- Portable functions:
-- ----------------------------------------------------------------------------

startNewProcess :: FilePath -> [String] -> IO (Either IOException (T.ExitCode, String, String))
startNewProcess prog args = liftIO $ try (readCreateProcessWithExitCode (proc prog args) "")

getTermWidth :: IO (Maybe Int)
getTermWidth = do
    isTerminal <- hIsTerminalDevice stdout
    if isTerminal
        then fmap Terminal.Size.width <$> Terminal.Size.size
        else pure Nothing

setFileMode :: FilePath -> FileMode -> IO ()
setFileMode fp fm = PC.setFileMode fp fm

-- ----------------------------------------------------------------------------
-- Functions with different definitions for Windows and Posix systems:
-- ----------------------------------------------------------------------------

executeFile :: FilePath -> [String] -> Maybe [(String, String)] -> IO ()
#if defined(mingw32_HOST_OS)
executeFile prog args env = do
    exit_code <- withCreateProcess
                 ((proc prog args) { env = env, delegate_ctlc = True }) $
                  \_ _ _ p -> waitForProcess p
    case exit_code of
      ExitSuccess   -> exitSuccess
      ExitFailure _ -> exitFailure
#else
executeFile prog args env = PP.executeFile prog True args env
#endif

unsupportedOnWin :: MonadLogger m => m (Either e ()) -> m (Either e ())
#if defined(mingw32_HOST_OS)
unsupportedOnWin _op = Right <$> logWarnN "This operation is not supported on Windows."
#else
unsupportedOnWin = id
#endif

isSudo :: MonadIO m => m Bool
#if defined(mingw32_HOST_OS)
isSudo = return False
#else
isSudo = liftIO $ isJust <$> lookupEnv "SUDO_COMMAND"
#endif

-- This function runs an SDK Assistant installer (e.g. for self upgrade)
runInstaller :: (MonadIO m) => T.FilePath -> FilePathOfDaBinary -> m ()
#if defined(mingw32_HOST_OS)
runInstaller installerPath actualBinWrapped = do
    let actualBin = unwrapFilePathOf actualBinWrapped
    whenM (T.testfile actualBin) $ liftIO $ do
        timeTag <- formatTime defaultTimeLocale "%F-%H-%M-%S" <$> getCurrentTime
        let oldTag = "old-" <> timeTag
        T.mv actualBin (actualBin T.<.> (pack oldTag))
        T.mv installerPath actualBin
#else
runInstaller installerPath _actualBin =
    liftIO $ callProcess "/bin/sh" [pathToString installerPath]
#endif

removeDaHomeDir :: FilePathOfDaHomeDir -> FilePathOfDaBinary -> IO ()
#if defined(mingw32_HOST_OS)
removeDaHomeDir daSdkDir actualBinWrapped = do
    dirExists <- LT.testdir daSdkDir
    when dirExists $ do
        -- First, we move da.exe (the exe cannot delete itself) to remove it from PATH ASAP
        -- Then, we delete the DA SDK folder
        -- Finally, we delete the moved da.exe after exiting from this process
        let deletableCopy = unwrapFilePathOf daSdkDir T.</> "../~da-assistant.exe"
        T.mv _actualBin deletableCopy
        void $ T.shell (pack $ "rmdir /S /Q " <> daSdkDirStr) T.empty
        let command = "timeout /T 2 /NOBREAK > nul & del /F " <> pathToString deletableCopy <> " > nul"
        void $ createProcess (proc "cmd.exe" ["/Q", "/S", "/C", "cmd /s /c " <> command]) { detach_console = True }
#else
removeDaHomeDir daSdkDir actualBinWrapped = void $ T.shell (pack $ "rm -rf " <> daSdkDirStr) T.empty
#endif
  where
    daSdkDirStr = pathOfToString daSdkDir
    _actualBin = unwrapFilePathOf actualBinWrapped

runShellScript :: FilePath -> [String] -> Maybe [(String, String)] -> IO ()
#if defined(mingw32_HOST_OS)
runShellScript _script _args _env = return ()
#else
runShellScript = executeFile
#endif

linkOrCopy' :: Bool -> L.FilePathOf t -> T.FilePath -> IO (Either SomeException ())
linkOrCopy' force a b = linkOrCopy force (unwrapFilePathOf a) b

linkOrCopy :: Bool -> T.FilePath -> T.FilePath -> IO (Either SomeException ())
linkOrCopy force a b = try $ do
    when force $ do
        exists <- T.testpath b
        when exists $ T.rmtree b
#if defined(mingw32_HOST_OS)
    T.cptree a b
#else
    D.createFileLink (pathToString a) (pathToString b)
#endif

createBinaryStarter :: T.FilePath -> T.FilePath -> IO ()
#if defined(mingw32_HOST_OS)
createBinaryStarter binaryPath daBinDirBinTarget = do
    let daBinDirBinTarget' = daBinDirBinTarget T.<.> "cmd"
        runnerScript = "START " <> pathToString binaryPath <> " %*"
    writeFile (pathToString daBinDirBinTarget') runnerScript
#else
createBinaryStarter binaryPath daBinDirBinTarget = do
    let runnerScript = "#!/usr/bin/env sh\n"
                    <> pathToString binaryPath <> " \"$@\""
    writeFile (pathToString daBinDirBinTarget) runnerScript
#endif

allBinaryExtensions :: [Maybe Text]
allBinaryExtensions =
    unixBinaryExtensions <> windowsBinaryExtensions

osSpecificBinaryExtensions :: [Maybe Text]
#if defined(mingw32_HOST_OS)
osSpecificBinaryExtensions = windowsBinaryExtensions
#else
osSpecificBinaryExtensions = unixBinaryExtensions
#endif    

unixBinaryExtensions :: [Maybe Text]
unixBinaryExtensions = [Nothing]

windowsBinaryExtensions :: [Maybe Text]
windowsBinaryExtensions = [Just "cmd", Just "bar", Just "exe"]

runAsDaemonProcess :: L.FilePathOfProcDir -> T.FilePath -> T.FilePath -> [String] -> (Pid -> IO ()) -> IO ()
#if defined(mingw32_HOST_OS)
runAsDaemonProcess _procPath _logPath _command _args _writePidFile =
    putStrLn "runAsDaemonProcess: unsupported under Windows."
#else
runAsDaemonProcess procPath logPath command args writePidFile =
    void $ liftIO $ forkProcess $ do
        -- Create a new session so reassociate the process to init.
        void createSession

        -- Fork again to make sure control terminal is not reacquired.
        pid <- forkProcess $ do
            remapFds $ pathToString logPath
            void $ PP.executeFile (pathToString command) False args Nothing

        -- Write out the process description to our pid file
        LT.mktree procPath
        writePidFile pid

remapFds :: FilePath -> IO ()
remapFds logPath = do
  -- Close all inherited file descriptors
  forM_ [0..1023] $ \fd ->
      PIO.closeFd fd `catch` (\(_ex :: SomeException) -> return ())
  -- Open /dev/null which will become FD 0 (STDIN) because 0..1023 are closed and the resulting FD
  -- should be the lowest-numbered file descriptor not currently open for the process
  _nullFd <- PIO.openFd "/dev/null" PIO.ReadOnly Nothing PIO.defaultFileFlags
  -- Open FD 1 (STDOUT)
  outFd <- PIO.createFile logPath (PIO.ownerReadMode `PIO.unionFileModes` PIO.ownerWriteMode)
  -- We need FD 2 (STDERR) to be the dup of FD 1 (STDOUT) for logging
  void $ PIO.dupTo outFd PIO.stdError
#endif

installationDir :: MonadIO m => m L.FilePathOfInstallationDir
installationDir = L.FilePathOf <$>
#if defined(mingw32_HOST_OS)
    getShellFolder cSIDL_LOCAL_APPDATA
#else
    (T.home >>= T.realpath)
#endif

daDir :: T.FilePath
daDir =
#if defined(mingw32_HOST_OS)
    "damlsdk"
#else
    ".da"
#endif

daRelativeBinDir :: T.FilePath
daRelativeBinDir =
#if defined(mingw32_HOST_OS)
    "cli"
#else
    "bin"
#endif

daRelativeBinName :: T.FilePath
daRelativeBinName =
#if defined(mingw32_HOST_OS)
    "da.exe"
#else
    "da"
#endif