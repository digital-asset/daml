-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.DocTest (docTest) where

import Control.Monad
import DA.Daml.Options.Types
import DA.Daml.DocTest
import qualified Data.Set as Set
import qualified Data.Text.Extended as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Shake
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import qualified Development.Shake as Shake
import System.Directory
import System.Exit
import System.FilePath

-- We might want merge the logic for `damlc doctest` and `damlc test`
-- at some point but the requirements are slightly different (e.g., we
-- will eventually want to remap source locations in `damlc doctest`
-- to the doc comment) so for now we keep them separate.

docTest :: IdeState -> [NormalizedFilePath] -> IO ()
docTest ideState files = do
    ms <- runAction ideState (uses_ GenerateDocTestModule files)
    let docTestFile m = toNormalizedFilePath $
            genDir </>
            T.unpack (T.replace "." "/" (docTestModuleName $ genModuleName m)) -<.>
            "daml"
    let msWithPaths = map (\m -> (m, docTestFile m)) ms
    forM_ msWithPaths $ \(m, path) -> do
        createDirectoryIfMissing True (takeDirectory $ fromNormalizedFilePath path)
        T.writeFileUtf8 (fromNormalizedFilePath path) (genModuleContent m)
    setFilesOfInterest ideState (Set.fromList $ map snd msWithPaths)
    runAction ideState $ do
        void $ Shake.forP msWithPaths $ \(_, path) -> use_ RunScenarios path
    -- This seems to make the gRPC issues on shutdown slightly less
    -- frequent but sadly it doesn’t make them go away completely.
    runActionSync ideState (pure ())
    diags <- getDiagnostics ideState
    when (any ((Just DsError ==) . _severity . snd) diags) exitFailure

