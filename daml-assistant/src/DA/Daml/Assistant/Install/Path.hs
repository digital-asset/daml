-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}
module DA.Daml.Assistant.Install.Path
    ( updatePath
    ) where

import Control.Monad
import System.FilePath

#ifndef mingw32_HOST_OS
import System.Directory
import System.Environment
import System.IO
import Data.Maybe
#endif

#ifdef mingw32_HOST_OS
import Control.Exception.Safe
import Data.List.Extra
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.Storable
import Graphics.Win32.GDI.Types
import Graphics.Win32.Message
import Graphics.Win32.Window.PostMessage
import System.Win32.Registry hiding (regQueryValueEx)
import System.Win32.Types
#endif

import DA.Daml.Assistant.Types

updatePath :: InstallOptions -> (String -> IO ()) -> FilePath -> IO ()
#ifdef mingw32_HOST_OS
updatePath installOpts output targetPath
    | SetPath No <- iSetPath installOpts = suggestAddToPath output targetPath
    | otherwise = do
    -- Updating PATH on Windows is annoying so we do it automatically.
    bracket (regOpenKeyEx hKEY_CURRENT_USER "Environment" kEY_ALL_ACCESS) regCloseKey $ \envKey -> do
        -- We use getSearchPath for the check instead of reading from the registry
        -- since that allows users to modify the environment variable to temporarily
        -- add it to PATH.
        searchPaths <- map dropTrailingPathSeparator <$> getSearchPath
        oldPath <- splitSearchPath <$> regQueryStringValue envKey "Path"
        when (targetPath `notElem` searchPaths && targetPath `notElem` oldPath) $ do
            let newPath = intercalate [searchPathSeparator] $ targetPath : oldPath
            regSetStringValue envKey "Path" newPath
            -- Ask applications to pick up the change.
            _ <-
                withTString "Environment" $ \ptr ->
                alloca $ \lpdwResult ->
                let IntPtr ptr' = ptrToIntPtr ptr
                in sendMessageTimeout
                       hWND_BROADCAST
                       wM_WININICHANGE
                       0
                       (CIntPtr $ fromIntegral ptr')
                       0 -- SMTO_NORMAL
                       5000
                       lpdwResult
            output (targetPath <> " has been added to your PATH..")

#include "windows_cconv.h"

foreign import WINDOWS_CCONV "windows.h SendMessageTimeoutW"
    sendMessageTimeout :: HWND -> WindowMessage -> WPARAM -> LPARAM -> UINT -> UINT -> Ptr (Ptr DWORD) -> IO LRESULT

-- Not exposed by Win32 so we duplicate it.
foreign import WINDOWS_CCONV unsafe "windows.h RegQueryValueExW"
  c_RegQueryValueEx :: PKEY -> LPCTSTR -> Ptr DWORD -> Ptr DWORD -> LPBYTE -> Ptr DWORD -> IO ErrCode

-- | Win32 does expose `regQueryValueEx` but it requires you to know the
-- size of the value in advance which makes it quite annoying to use.
regQueryStringValue :: HKEY -> String -> IO String
regQueryStringValue key valueName =
    withForeignPtr key $ \p_key ->
    withTString valueName $ \c_valueName ->
    alloca $ \p_valueLen -> do
        failUnlessSuccess "regQueryDefaultValue" $
            c_RegQueryValueEx p_key c_valueName nullPtr nullPtr nullPtr p_valueLen
        valueLen <- peek p_valueLen
        allocaArray0 (fromIntegral valueLen) $ \ c_value -> do
            failUnlessSuccess "regQueryDefaultValue" $
                c_RegQueryValueEx p_key c_valueName nullPtr nullPtr c_value p_valueLen
            peekTString (castPtr c_value)
#else
updatePath installOpts output targetPath
  | SetPath No <- iSetPath installOpts = suggestAddToPath output targetPath
  | SetPath autoOrYes <- iSetPath installOpts = do
    searchPaths <- map dropTrailingPathSeparator <$> getSearchPath
    unless (targetPath `elem` searchPaths) $ do
      mbShellPath <- lookupEnv "SHELL"
      let mbShellConfig = do
            shellPath <- mbShellPath
            shellConfig (takeFileName shellPath) targetPath
      case mbShellConfig of
        Nothing -> output setPathManualMsg
        Just (configFile, cmd) -> do
          home <- getHomeDirectory
          let cfgFile = home </> configFile
          cfgFileExists <- doesFileExist cfgFile
          if cfgFileExists
            then do
              content <- readFile cfgFile
              unless (cmd `elem` lines content) $ do
                case autoOrYes of
                  Auto -> do
                    stdinIsTerminal <- hIsTerminalDevice stdin
                    stdoutIsTerminal <- hIsTerminalDevice stdout
                    ci <- isCI
                    if stdinIsTerminal && stdoutIsTerminal && not ci
                      then do
                        answer <- prompt output ("Add Daml assistant executable to your PATH (in " ++ configFile ++ ")?") "Yes" ["No"]
                        when (answer `elem` ["Yes", "yes", "y", "Y"]) $ doUpdatePath cfgFile cmd
                      else output setPathManualMsg
                  Yes -> doUpdatePath cfgFile cmd
            else output setPathManualMsg
  where
    setPathManualMsg = "Please add " <> targetPath <> " to your PATH."
    doUpdatePath cfg cmd = do
      appendFile cfg cmd
      output $
        "Your " <> cfg <>
        " has been updated. You need to logout and login again for the change to take effect."

prompt :: (String -> IO ()) -> String -> String -> [String] -> IO String
prompt output msg def others = do
  output $ msg <> (unwords $ " [" <> def <> "]" : others)
  ans <- getLine
  return $
    if null ans
      then def
      else ans

isCI :: IO Bool
isCI = isJust <$> lookupEnv "CI"

shellConfig :: String -> FilePath -> Maybe (FilePath, String)
shellConfig shell targetPath =
  case shell of
    "zsh" -> Just (".zprofile", "export PATH=$PATH:" <> targetPath)
    "bash" -> Just (".bash_profile", "export PATH=$PATH:" <> targetPath)
    "sh" -> Just (".profile", "export PATH=$PATH:" <> targetPath)
    _other -> Nothing

#endif

suggestAddToPath :: (String -> IO ()) -> FilePath -> IO ()
suggestAddToPath output targetPath = do
    -- Ask user to add .daml/bin to PATH if it is absent.
    searchPaths <- map dropTrailingPathSeparator <$> getSearchPath
    when (targetPath `notElem` searchPaths) $ do
        output ("Please add " <> targetPath <> " to your PATH.")
