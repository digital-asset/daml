-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Development.IDE.GHC.Warnings(withWarnings) where

import GhcMonad
import ErrUtils
import GhcPlugins as GHC hiding (Var)

import           Control.Concurrent.Extra
import           Control.Monad.Extra
import qualified           Data.Text as T

import           Development.IDE.Types.Diagnostics
import Development.IDE.GHC.Util
import           Development.IDE.GHC.Error


-- | Take a GHC monadic action (e.g. @typecheckModule pm@ for some
-- parsed module 'pm@') and produce a "decorated" action that will
-- harvest any warnings encountered executing the action. The 'phase'
-- argument classifies the context (e.g. "Parser", "Typechecker").
--
--   The ModSummary function is required because of
--   https://github.com/ghc/ghc/blob/5f1d949ab9e09b8d95319633854b7959df06eb58/compiler/main/GHC.hs#L623-L640
--   which basically says that log_action is taken from the ModSummary when GHC feels like it.
--   The given argument lets you refresh a ModSummary log_action
withWarnings :: GhcMonad m => T.Text -> ((ModSummary -> ModSummary) -> m a) -> m ([FileDiagnostic], a)
withWarnings diagSource action = do
  warnings <- liftIO $ newVar []
  oldFlags <- getDynFlags
  let newAction dynFlags _ _ loc _ msg = do
        let d = diagFromErrMsg diagSource dynFlags $ mkPlainWarnMsg dynFlags loc msg
        modifyVar_ warnings $ return . (d:)
  setLogAction newAction
  res <- action $ \x -> x{ms_hspp_opts = (ms_hspp_opts x){log_action = newAction}}
  setLogAction $ log_action oldFlags
  warns <- liftIO $ readVar warnings
  return (reverse $ concat warns, res)

setLogAction :: GhcMonad m => LogAction -> m ()
setLogAction act = void $ modifyDynFlags $ \dyn -> dyn{log_action = act}
