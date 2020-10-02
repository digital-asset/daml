-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}
module DA.Daml.Assistant.Install.Path
    ( updatePath
    ) where

import Control.Monad
import System.FilePath

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
    | SetPath b <- iSetPath installOpts, not b = suggestAddToPath output targetPath
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
updatePath _ output targetPath = suggestAddToPath output targetPath
#endif

suggestAddToPath :: (String -> IO ()) -> FilePath -> IO ()
suggestAddToPath output targetPath = do
    -- Ask user to add .daml/bin to PATH if it is absent.
    searchPaths <- map dropTrailingPathSeparator <$> getSearchPath
    when (targetPath `notElem` searchPaths) $ do
        output ("Please add " <> targetPath <> " to your PATH.")
