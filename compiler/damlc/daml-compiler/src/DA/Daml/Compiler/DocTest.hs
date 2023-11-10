-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.DocTest (docTest) where

import Control.Monad
import DA.Daml.DocTest
import DA.Daml.Options.Types
import Data.HashSet qualified as HashSet
import Data.Text.Extended qualified as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Development.Shake qualified as Shake
import System.Directory
import System.Exit
import System.FilePath

-- We might want merge the logic for `damlc doctest` and `damlc test`
-- at some point but the requirements are slightly different (e.g., we
-- will eventually want to remap source locations in `damlc doctest`
-- to the doc comment) so for now we keep them separate.

docTest :: IdeState -> [NormalizedFilePath] -> IO ()
docTest ideState files = do
    ms <- runActionSync ideState (uses_ GenerateDocTestModule files)
    let docTestFile m = toNormalizedFilePath' $
            genDir </>
            T.unpack (T.replace "." "/" (docTestModuleName $ genModuleName m)) -<.>
            "daml"
    let msWithPaths = map (\m -> (m, docTestFile m)) ms
    forM_ msWithPaths $ \(m, path) -> do
        createDirectoryIfMissing True (takeDirectory $ fromNormalizedFilePath path)
        T.writeFileUtf8 (fromNormalizedFilePath path) (genModuleContent m)
    setFilesOfInterest ideState (HashSet.fromList $ map snd msWithPaths)

    runActionSync ideState $ do
        void $ Shake.forP msWithPaths $ \(_, path) -> use_ RunScripts path
    diags <- getDiagnostics ideState
    when (any (\(_, _, diag) -> Just DsError == _severity diag) diags) exitFailure
