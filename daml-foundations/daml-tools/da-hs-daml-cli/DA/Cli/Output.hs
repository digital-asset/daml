-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module DA.Cli.Output
  ( writeOutput
  , writeOutputBSL
  , reportErr
  , printDiagnostics
  ) where

import qualified Data.ByteString.Lazy                           as BSL
import           Data.String                                    (IsString)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Development.IDE.Types.Diagnostics
import Data.List.Extra
import           System.IO                                      (Handle, hClose, hPutStr, stdout, openFile, IOMode (WriteMode))
import           Control.Exception (bracket)

-- | Write some text to the destination specified on the command line.
--
--   If this was a regular file then write it there,
--   otherwise if the command line specified '-' as the file then write
--   the text to stdout.
writeOutputWith :: (IsString a) => (Handle -> a -> IO ()) -> FilePath -> a -> IO ()
writeOutputWith write outFile output =
    bracket open close $ \handle -> do
      write handle output
      write handle "\n"
  where
    useStdOut = outFile == "-"

    open
      | useStdOut = return stdout
      | otherwise = openFile outFile WriteMode

    close handle
      | useStdOut = return ()
      | otherwise = hClose handle

writeOutput :: FilePath -> String -> IO ()
writeOutput = writeOutputWith hPutStr

writeOutputBSL :: FilePath -> BSL.ByteString -> IO ()
writeOutputBSL = writeOutputWith BSL.hPutStr


reportErr :: String -> [FileDiagnostic] -> IO a
reportErr msg errs =
  ioError $
  userError $
  unlines
    [ msg
    , T.unpack $
      showDiagnosticsColored $ nubOrd errs
    ]

printDiagnostics :: [FileDiagnostic] -> IO ()
printDiagnostics [] = return ()
printDiagnostics xs = T.putStrLn $ showDiagnosticsColored xs
