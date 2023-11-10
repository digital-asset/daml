-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
module DA.Daml.Compiler.Repl
    ( newReplLogger
    , runRepl
    , ReplLogger(..)
    ) where

import FastString
import TysWiredIn (unitDataCon, unitTyCon)
import BasicTypes (Boxity(..), PromotionFlag(..), Origin(..))
import DynFlags
import Bag (bagToList, unitBag)
import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.Extra
import Control.Exception.Safe
import Control.Monad.Except
import Control.Monad.Extra
import Control.Monad.State.Strict qualified as State
import Control.Monad.Trans.Maybe
import DA.Daml.Compiler.Output (printDiagnostics)
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.InferSerializability qualified as Serializability
import DA.Daml.LF.Simplifier qualified as LF
import DA.Daml.LF.TypeChecker qualified as LF
import DA.Daml.LF.ReplClient qualified as ReplClient
import DA.Daml.LFConversion (convertModule)
import DA.Daml.Options.Types
import DA.Daml.Preprocessor.Records qualified as Preprocessor
import DA.Daml.UtilGHC
import DA.Daml.UtilLF (buildPackage)
import Data.Bifunctor (first)
import Data.Either.Combinators (whenLeft)
import Data.Functor.Alt
import Data.Functor.Bind
import Data.Foldable
import Data.Generics.Uniplate.Data (descendBi)
import Data.IORef
import Data.List (intercalate)
import Data.Map.Strict qualified as Map
import Data.Maybe
import Data.NameMap qualified as NM
import Data.Semigroup (Last(..))
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Development.IDE.Core.API
import Development.IDE.Core.Compile (compileModule, typecheckModule, RunSimplifier(..))
import Development.IDE.Core.RuleTypes
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules.Daml (diagsToIdeResult, getExternalPackages, ideErrorPretty)
import Development.IDE.Core.Service
import Development.IDE.Core.Service.Daml (DamlEnv(..), getDamlServiceEnv)
import Development.IDE.Core.Shake
import Development.IDE.GHC.Util
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import Development.IDE.Types.Options
import ErrUtils
import GHC hiding (typecheckModule)
import GHC.LanguageExtensions.Type
import HscTypes (HscEnv(..), HscSource(HsSrcFile), HomeModInfo(hm_iface))
import Language.Haskell.GhclibParserEx.Parse
import Language.LSP.Types qualified as LSP
import Module (mainUnitId, unitIdString)
import OccName
import Outputable (ppr, showSDoc)
import Outputable qualified
import RdrName (getRdrName, mkRdrUnqual)
import SrcLoc
import System.Console.Repline qualified as Repl
import System.Exit
import System.IO.Extra
import TcEvidence (idHsWrapper)
import Type (splitTyConApp)

data Error
    = ParseError MsgDoc
    | UnsupportedStatement String -- ^ E.g., pattern on the LHS
    | ExpectedExpression String
    | NotImportedModules [ModuleName]
    | TypeError -- ^ The actual error will be in the diagnostics
    | ScriptBackendError ReplClient.BackendError
    | ScriptError T.Text
    | InternalError T.Text

renderError :: DynFlags -> Error -> IO ()
renderError dflags err = case err of
    ParseError err ->
        putStrLn (showSDoc dflags err)
    (UnsupportedStatement str) ->
        putStrLn ("Unsupported statement: " <> str)
    (ExpectedExpression str) ->
        putStrLn ("Expected an expression but got: " <> str)
    (NotImportedModules names) ->
        putStrLn ("Not imported, cannot remove: " <> intercalate ", " (map moduleNameString names))
    TypeError ->
        -- The error will be displayed via diagnostics.
        pure ()
    (ScriptError err) -> T.putStrLn err
    (ScriptBackendError err) -> print err
    (InternalError err) -> T.putStrLn err

-- | Take a set of variables and a pattern and shadow all the variables
-- in the pattern by turning them into wildcard patterns.
shadowPat :: OccSet -> LPat GhcPs -> LPat GhcPs
shadowPat vars p
  = go (unLoc p)
  where
    go p@(VarPat _ var)
      | occName (unLoc var) `elemOccSet` vars = WildPat noExt
      | otherwise = p
    go p@(WildPat _) = p
    go (LazyPat ext pat) = LazyPat ext (go pat)
    go (BangPat ext pat) = BangPat ext (go pat)
    go (AsPat ext a pat)
        | occName (unLoc a) `elemOccSet` vars = go pat
        | otherwise = AsPat ext a (go pat)
    go (ViewPat ext expr pat) = ViewPat ext expr (go pat)
    go (ParPat ext pat) = ParPat ext (go pat)
    go (ListPat ext pats) = ListPat ext (map go pats)
    go (TuplePat ext pats boxity) = TuplePat ext (map go pats) boxity
    go (SumPat ext pat tag arity) = SumPat ext (go pat) tag arity
    go (ConPatIn ext ps) = ConPatIn ext (shadowDetails ps)
    go ConPatOut{} = error "ConPatOut is never produced by the parser"
    go p@LitPat{} = p
    go p@NPat{} = p
    go NPlusKPat{} = error "N+k patterns are not suppported"
    go (SigPat ext pat sig) = SigPat ext (go pat) sig
    go SplicePat {} = error "Daml does not support Template Haskell"
    go (CoPat ext wrap pat ty) = CoPat ext wrap (go pat) ty
    go (XPat locP) = XPat (fmap go locP)

    shadowDetails :: HsConPatDetails GhcPs -> HsConPatDetails GhcPs
    shadowDetails (PrefixCon ps) = PrefixCon (map go ps)
    shadowDetails (RecCon fs) =
        RecCon fs
            { rec_flds =
                  map (fmap (\f -> f { hsRecFieldArg = go (hsRecFieldArg f) }))
                      (rec_flds fs)
            }
    shadowDetails (InfixCon p1 p2) = InfixCon (go p1) (go p2)

-- Note [Partial Patterns]
-- A partial binding of the form
--
--     Just (x, y) <- pure (Nothing : Maybe (Int, Int))
--
-- should fail on the line itself rather than on a later line.
-- To accomplish this, we transform the statement into
--
-- (x, y) <- do
--   Just (x, y) <- pure (Nothing : Maybe (Int, Int))
--   pure (x, y)
--
-- That ensures that the line itself fails and it
-- avoids partial pattern match warnings on subsequent lines.

toTuplePat :: [RdrName] -> LPat GhcPs
toTuplePat [] = noLoc (WildPat noExt)
toTuplePat [v] = noLoc (VarPat noExt $ noLoc v)
toTuplePat vars = noLoc $
    TuplePat noExt [noLoc (VarPat noExt $ noLoc v) | v <- vars] Boxed

toTupleExpr :: LPat GhcPs -> LHsExpr GhcPs
toTupleExpr pat = case vars of
    [] -> noLoc $ HsVar noExt (noLoc $ getRdrName unitDataCon)
    [var] -> noLoc $ HsVar noExt (noLoc var)
    _ -> noLoc $ ExplicitTuple noExt [noLoc (Present noExt (noLoc $ HsVar noExt (noLoc v))) | v <- vars] Boxed
  where vars = collectPatBinders pat

-- | Type for the statements we support.
data SupportedStatement
    = BodyStatement (LHsExpr GhcPs)
    | BindStatement (LPat GhcPs) (LHsExpr GhcPs)
    | LetStatement LetBinding

data LetBinding
    = FunBinding (Located RdrName) (MatchGroup GhcPs (LHsExpr GhcPs))
    | PatBinding (LPat GhcPs) (GRHSs GhcPs (LHsExpr GhcPs))

toLocalBinds :: LetBinding -> LHsLocalBindsLR GhcPs GhcPs
toLocalBinds bind =
    noLoc $ HsValBinds noExt $
    ValBinds noExt (unitBag $ noLoc hsBind) []
  where
    hsBind = case bind of
        FunBinding f mg -> FunBind
          { fun_ext = noExt
          , fun_id = f
          , fun_matches = mg
          , fun_co_fn = idHsWrapper
          , fun_tick = []
          }
        PatBinding pat rhs -> PatBind
          { pat_ext = noExt
          , pat_lhs = pat
          , pat_rhs = rhs
          , pat_ticks = ([], [])
          }

validateStmt :: Stmt GhcPs (LHsExpr GhcPs) -> Maybe SupportedStatement
validateStmt (BodyStmt _ expr _ _) = Just (BodyStatement expr)
validateStmt (BindStmt _ pat expr _ _) = Just (BindStatement pat expr)
validateStmt (LetStmt _ (L _ (HsValBinds _ (ValBinds _ binds _))))
    -- We only support singleton binds for now. Anything else
    -- is annoying to write in the repl anyway.
    | [bind] <- bagToList binds = fmap LetStatement $ case unLoc bind of
          FunBind{..} -> Just (FunBinding fun_id fun_matches)
          PatBind{..} -> Just (PatBinding pat_lhs pat_rhs)
          _ -> Nothing
validateStmt _ = Nothing

stmtBoundVars :: SupportedStatement -> [RdrName]
stmtBoundVars (BodyStatement _) = []
stmtBoundVars (BindStatement pat _) = collectPatBinders pat
stmtBoundVars (LetStatement binding) = case binding of
    FunBinding f _ -> [unLoc f]
    PatBinding pat _ -> collectPatBinders pat

-- | Mapping from module name to all imports for that module.
--
-- Invariant: Imports for a module should be associated to its module name and
-- multiple imports of a module should never subsume each other, see
-- 'importInsert'.
--
-- This avoids redundant import lines and eases removing of module imports.
newtype Imports = Imports { getImports :: Map.Map ModuleName [ImportDecl GhcPs] }

-- | Add an import declaration.
--
-- If the new import declaration subsumes a previous import declaration, then
-- new one will replace the old one. If the new import declaration is subsumed
-- by an already existing import declaration, then it will not be added.
--
-- Subsumption of import declarations is based on the implementation of
-- 'GHCi.UI.iiSubsumes'. Note, that this is not fully precise. For example,
-- @import DA.Time (days, hours)@ should subsume @import DA.Time (hours)@, but
-- does not because of different source locations on the imported symbols.
importInsert :: ImportDecl GhcPs -> Imports -> Imports
importInsert i (Imports m) = Imports $ Map.alter insert name m
  where
    name = unLoc . ideclName $ i
    -- Based on 'GHCi.UI.addNotSubsumed'.
    insert Nothing = Just [i]
    insert (Just is)
      | any (`subsumes` i) is = Just is
      | otherwise = Just $! i : filter (not . (i `subsumes`)) is
    -- Based on 'GHCi.UI.iiSubsumes'.
    --
    -- Returns True if the left import subsumes the right one.
    d1 `subsumes` d2
      = unLoc (ideclName d1) == unLoc (ideclName d2)
        && ideclAs d1 == ideclAs d2
        && (not (ideclQualified d1) || ideclQualified d2)
        && (ideclHiding d1 `hidingSubsumes` ideclHiding d2)
    _                    `hidingSubsumes` Just (False, L _ []) = True
    Just (False, L _ xs) `hidingSubsumes` Just (False, L _ ys) = all (`elem` xs) ys
    h1                   `hidingSubsumes` h2                   = h1 == h2

importDelete :: ModuleName -> Imports -> Imports
importDelete name (Imports m) = Imports $ Map.delete name m

importMember :: ModuleName -> Imports -> Bool
importMember name (Imports m) = Map.member name m

importFromList :: [ImportDecl GhcPs] -> Imports
importFromList = foldl' (flip importInsert) (Imports Map.empty)

importToList :: Imports -> [ImportDecl GhcPs]
importToList (Imports imports) = concat $ Map.elems imports

data ReplState = ReplState
  { imports :: !Imports
  , bindings :: ![(LPat GhcPs, Type)]
  , lineNumber :: !Int
  }

type ReplM = Repl.HaskelineT (State.StateT ReplState IO)

data ReplInput
  = ReplStatement (Stmt GhcPs (LHsExpr GhcPs))
  | ReplImport (ImportDecl GhcPs)

parseReplInput :: String -> DynFlags -> Either Error ReplInput
parseReplInput input dflags =
    -- Short-circuit on the first successful parse.
    -- The most common input will be statements. So, we attempt parsing
    -- statements last to always emit statement parse errors on failure.
        (ReplImport . unLoc <$> tryParse (parseImport input dflags))
    <!> (ReplStatement . preprocess . unLoc <$> tryParse (parseStatement input dflags))
  where
    preprocess :: Stmt GhcPs (LHsExpr GhcPs) -> Stmt GhcPs (LHsExpr GhcPs)
    preprocess = descendBi Preprocessor.onExp
    tryParse :: ParseResult a -> Either Error a
    tryParse (POk _ result) = Right result
    tryParse (PFailed _ _ errMsg) = Left (ParseError errMsg)


-- | Load all packages in the given session.
--
-- Returns the list of modules in the specified import packages and an action that waits for them to be loaded by the repl client.
loadPackages :: [(LF.PackageName, Maybe LF.PackageVersion)] -> ReplClient.Handle -> IdeState -> IO ([ImportDecl GhcPs], IO ())
loadPackages importPkgs replClient ideState = do
    -- Load packages
    Just (PackageMap pkgs) <- runAction ideState (use GeneratePackageMap "Dummy.daml")
    Just stablePkgs <- runAction ideState (useNoFile GenerateStablePackages)
    t <- async $ do
      r <- ReplClient.loadPackages replClient (map LF.dalfPackageBytes $ toList pkgs <> toList stablePkgs)
      whenLeft r $ \err ->  do
         hPutStrLn stderr ("Package could not be loaded: " <> show err)
         exitFailure
    -- Determine module names in imported DALFs.
    let unversionedPkgs = Map.mapKeys (fst . LF.splitUnitId) pkgs
        toUnitId (pkgName, mbVersion) = pkgNameVersion pkgName mbVersion
        lookupPkg (pkgName, Nothing) = Map.lookup pkgName unversionedPkgs
        lookupPkg (toUnitId -> unitId) = Map.lookup unitId pkgs
    importLfPkgs <- forM importPkgs $ \importPkg ->
        case lookupPkg importPkg of
            Just dalf -> pure $ LF.extPackagePkg $ LF.dalfPackagePkg dalf
            Nothing -> do
                hPutStrLn stderr $
                    "Could not find package for import: " <> unitIdString (toUnitId importPkg) <> "\n"
                    <> "Known packages: " <> intercalate ", " (unitIdString <$> Map.keys pkgs)
                exitFailure
    let imports =
          [ simpleImportDecl . mkModuleName . T.unpack . LF.moduleNameString $ mod
          | pkg <- importLfPkgs
          , mod <- NM.names $ LF.packageModules pkg
          ]
    pure (imports, wait t)

data ReplLogger = ReplLogger
  { withReplLogger :: forall a. ([FileDiagnostic] -> IO ()) -> IO a -> IO a
  -- ^ Temporarily modify what happens to diagnostics
  , replEventLogger :: NotificationHandler
  -- ^ Logger to pass to `withDamlIdeState`
  }


newReplLogger :: IO ReplLogger
newReplLogger = do
    lock <- newLock
    diagsRef <- newIORef $ \diags -> printDiagnostics stdout diags
    let replEventLogger :: forall (m :: LSP.Method 'LSP.FromServer 'LSP.Notification). LSP.SMethod m -> LSP.MessageParams m -> IO ()
        replEventLogger
          LSP.STextDocumentPublishDiagnostics
          (LSP.PublishDiagnosticsParams (uriToFilePath' -> Just fp) _ (List diags)) = do
            logger <- readIORef diagsRef
            logger $ map (toNormalizedFilePath' fp, ShowDiag,) diags
        replEventLogger _ _ = pure ()
        withReplLogger :: ([FileDiagnostic] -> IO ()) -> IO a -> IO a
        withReplLogger logAct f =
            withLock lock $
            bracket
                (readIORef diagsRef <* atomicWriteIORef diagsRef logAct)
                (atomicWriteIORef diagsRef)
                (const f)
    pure ReplLogger{replEventLogger = NotificationHandler replEventLogger,..}

runRepl
    :: [(LF.PackageName, Maybe LF.PackageVersion)]
    -> Options
    -> ReplClient.Handle
    -> ReplLogger
    -> IdeState
    -> IO ()
runRepl importPkgs opts replClient logger ideState = do
    (imports, waitForPackages) <- loadPackages importPkgs replClient ideState
    -- Typecheck once to get the GlobalRdrEnv
    let initialLineNumber = 0
    dflags <- liftIO $
         hsc_dflags . hscEnv <$>
         runAction ideState (use_ GhcSession $ lineFilePath initialLineNumber)
    _ <-
        runExceptT (typecheckImports dflags (importFromList imports) initialLineNumber)
        >>= \case
        Left err -> do
            renderError dflags err
            exitFailure
        Right r -> pure r
    let initReplState = ReplState
          { imports = importFromList imports
          , bindings = []
          , lineNumber = 0
          }
    let replM = Repl.evalReplOpts Repl.ReplOpts
          { banner = const (pure "daml> ")
          , command = replLine waitForPackages
          , options = replOptions waitForPackages
          , prefix = Just ':'
          , multilineCommand = Nothing
          , tabComplete = Repl.Cursor $ \_ _ -> pure []
          , initialiser = pure ()
          , finaliser = do
                  liftIO $ putStrLn "Goodbye."
                  pure Repl.Exit
          }
    State.evalStateT replM initReplState
  where
    handleStmt
        :: IO ()
        -> DynFlags
        -> String
        -> Stmt GhcPs (LHsExpr GhcPs)
        -> ReplClient.ReplResponseType
        -> ExceptT Error ReplM ()
    handleStmt waitForPackages dflags line stmt rspType = do
        ReplState {imports, bindings, lineNumber} <- State.get
        supportedStmt <- maybe (throwError (UnsupportedStatement line)) pure (validateStmt stmt)
        let rendering = renderModule imports lineNumber bindings supportedStmt
        (lfMod, tmrModule -> tcMod) <- printDelayedDiagnostics $ case (rspType, rendering) of
            (ReplClient.ReplText, BindingRendering t) ->
                tryTypecheck dflags lineNumber t
            (ReplClient.ReplText, BodyRenderings {..}) ->
                withExceptT getLast
                $   withExceptT Last (tryTypecheck dflags lineNumber unitScript)
                <!> withExceptT Last (tryTypecheck dflags lineNumber printableScript)
                <!> withExceptT Last (tryTypecheck dflags lineNumber arbitraryScript)
                <!> withExceptT Last (tryTypecheck dflags lineNumber purePrintableExpr)
            (ReplClient.ReplJson, BindingRendering _) ->
                throwError (ExpectedExpression line, [])
            (ReplClient.ReplJson, BodyRenderings {..}) ->
                withExceptT getLast
                $   withExceptT Last (tryTypecheck dflags lineNumber arbitraryScript)
                <!> withExceptT Last (tryTypecheck dflags lineNumber pureArbitraryExpr)
        -- Type of the statement so we can give it a type annotation
        -- and avoid incurring a typeclass constraint.
        stmtTy <- maybe (throwError TypeError) pure (exprTy $ tm_typechecked_source tcMod)
        -- If we get an error we don’t increment lineNumber and we
        -- do not get a new binding
        liftIO waitForPackages
        result <- withExceptT ScriptBackendError $ ExceptT $ liftIO $
            ReplClient.runScript replClient (optDamlLfVersion opts) lfMod rspType

        case result of
            ReplClient.ScriptSuccess mbResult -> liftIO $ whenJust mbResult T.putStrLn
            ReplClient.ScriptError err -> throwError (ScriptError err)
            ReplClient.InternalError err -> throwError (InternalError err)

        let boundVars = stmtBoundVars supportedStmt
            boundVars' = mkOccSet $ map occName boundVars
        State.modify' $ \s -> s
          { imports = imports
          , bindings = map (first (shadowPat boundVars')) bindings <> [(toTuplePat boundVars, stmtTy)]
          , lineNumber = lineNumber + 1
          }
    printDelayedDiagnostics :: MonadIO m => ExceptT (e, [[FileDiagnostic]]) m a -> ExceptT e m a
    printDelayedDiagnostics e = ExceptT $ do
        r <- runExceptT e
        case r of
            Left (err, diags) -> do
                liftIO $ mapM_ (printDiagnostics stdout) diags
                pure (Left err)
            Right r -> pure (Right r)
    tryTypecheck :: (MonadIO m, MonadError (Error, [[FileDiagnostic]]) m) => DynFlags -> Int -> ParsedSource -> m (LF.Module, TcModuleResult)
    tryTypecheck dflags lineNumber source = do
        let file = lineFilePath lineNumber
        -- liftIO $ setBufferModified ideState file $ Just t
        -- We need to temporarily suppress diagnostics since we use type errors
        -- to decide what to do. If a case succeeds we immediately print all diagnostics.
        -- If it fails, we return them and only print them once everything failed.
        diagsRef <- liftIO $ newIORef id
        -- here we don't want to use the `useE` function that uses cached results
        let useE' k = MaybeT . use k
        let writeDiags diags = atomicModifyIORef diagsRef (\f -> (f . (diags:), ()))
        let handleIdeResult :: IdeResult r -> MaybeT Action r
            handleIdeResult (diags, r) = do
                liftIO $ writeDiags diags
                MaybeT (pure r)
        r <- liftIO $ withReplLogger logger writeDiags $ runAction ideState $ runMaybeT $ do
            DamlEnv{envDamlLfVersion = lfVersion, envEnableScenarios, envAllowLargeTuples} <- lift getDamlServiceEnv
            let pm = toParsedModule dflags source
            IdeOptions { optDefer = defer } <- lift getIdeOptions
            packageState <- hscEnv <$> useE' GhcSession file
            tm <- handleIdeResult =<< liftIO (typecheckModule defer packageState [] pm)
            (safeMode, cgGuts, details) <- handleIdeResult =<< liftIO (compileModule (RunSimplifier False) packageState [] tm)
            let core = cgGutsToCoreModule safeMode cgGuts details
            PackageMap pkgMap <- useE' GeneratePackageMap file
            stablePkgs <- lift $ useNoFile_ GenerateStablePackages
            let modIface = hm_iface (tmrModInfo tm)
            case convertModule lfVersion envEnableScenarios envAllowLargeTuples pkgMap (Map.map LF.dalfPackageId stablePkgs) file core modIface details of
                Left diag -> handleIdeResult ([diag], Nothing)
                Right (v, conversionWarnings) -> do
                   pkgs <- lift $ getExternalPackages file
                   let pkgMeta = LF.PackageMetadata
                          { packageName = fromMaybe (LF.PackageName "repl") (optMbPackageName opts)
                          , packageVersion = fromMaybe (LF.PackageVersion "0.0.0") (optMbPackageVersion opts)
                          , upgradedPackageId = Nothing
                          }
                   let world = LF.initWorldSelf pkgs (buildPackage pkgMeta lfVersion [])
                   let simplified = LF.simplifyModule world lfVersion v
                   case Serializability.inferModule world lfVersion simplified of
                       Left err -> handleIdeResult (conversionWarnings ++ [ideErrorPretty file err], Nothing)
                       Right dalf -> do
                           let (_diags, checkResult) = diagsToIdeResult file $ LF.checkModule world lfVersion dalf
                           case checkResult of
                               Nothing -> MaybeT (pure Nothing)
                               Just () -> pure (dalf, tm)
        diags <- liftIO $ ($ []) <$> readIORef diagsRef
        case r of
            Nothing -> throwError (TypeError, diags)
            Just r -> do
                liftIO $ mapM_ (printDiagnostics stdout) diags
                pure r
    typecheckImports dflags imports line =
        printDelayedDiagnostics $
        tryTypecheck
            dflags
            line
            (buildModule (lineModuleName line) imports [])
    handleImport
        :: DynFlags
        -> ImportDecl GhcPs
        -> ExceptT Error ReplM ()
    handleImport dflags imp = addImports dflags [imp]
    replLine :: IO () -> String -> ReplM ()
    replLine waitForPackages line = do
        ReplState {lineNumber} <- State.get
        dflags <- liftIO $
            hsc_dflags . hscEnv <$>
            runAction ideState (use_ GhcSession $ lineFilePath lineNumber)
        r <- runExceptT $ do
            input <- ExceptT $ pure $ parseReplInput line dflags
            case input of
                ReplStatement stmt -> handleStmt waitForPackages dflags line stmt ReplClient.ReplText
                ReplImport imp -> handleImport dflags imp
        case r of
            Left err -> liftIO $ renderError dflags err
            Right () -> pure ()

    mkReplOption
        :: (DynFlags -> [String] -> ExceptT Error ReplM ())
        -> String -> ReplM ()
    mkReplOption option args = do
        ReplState {lineNumber} <- State.get
        dflags <- liftIO $
            hsc_dflags . hscEnv <$>
            runAction ideState (use_ GhcSession $ lineFilePath lineNumber)
        r <- runExceptT $ option dflags (words args)
        case r of
            Left err -> liftIO $ renderError dflags err
            Right () -> pure ()
    replOptions :: IO () -> Repl.Options ReplM
    replOptions waitForPackages =
      [ ("help", mkReplOption optHelp)
      , ("json", mkReplOption (optJson waitForPackages))
      , ("module", mkReplOption optModule)
      , ("show", mkReplOption optShow)
      ]
    optHelp _dflags _args = liftIO $ T.putStrLn $ T.unlines
      [ " Commands available from the prompt:"
      , ""
      , "   <statement>                 evaluate/run <statement>"
      , "   :json <expression>          evaluate/run <expression> and print the result in JSON format"
      , "   :module [+/-] <mod> ...     add or remove the modules from the import list"
      , "   :show imports               show the current module imports"
      ]
    optJson waitForPackages dflags (unwords -> line) = do
        input <- ExceptT $ pure $ parseReplInput line dflags
        case input of
            ReplStatement stmt -> handleStmt waitForPackages dflags line stmt ReplClient.ReplJson
            ReplImport _ -> throwError (ExpectedExpression line)
    optModule dflags ("-" : names) =
        removeImports dflags $ map mkModuleName names
    optModule dflags ("+" : names) =
        addImports dflags $ map (simpleImportDecl . mkModuleName) names
    optModule dflags names =
        addImports dflags $ map (simpleImportDecl . mkModuleName) names
    optShow dflags ["imports"] = do
        ReplState {imports} <- State.get
        liftIO $ putStr $ unlines $ moduleImports dflags imports
    optShow _dflags _ = liftIO $ putStrLn ":show [imports]"

    addImports
        :: DynFlags
        -> [ImportDecl GhcPs]
        -> ExceptT Error ReplM ()
    addImports dflags additional = do
        ReplState {imports, lineNumber} <- State.get
        let newImports = foldl' (flip importInsert) imports additional
        _ <- typecheckImports dflags newImports lineNumber
        State.modify $ \s -> s
            { imports = newImports
            }
    removeImports
        :: DynFlags
        -> [ModuleName]
        -> ExceptT Error ReplM ()
    removeImports dflags modules = do
        ReplState {imports, lineNumber} <- lift State.get
        let unknown = [name | name <- modules, not $ name `importMember` imports]
            newImports = foldl' (flip importDelete) imports modules
        unless (null unknown) $
            throwError $ NotImportedModules unknown
        _ <- typecheckImports dflags newImports lineNumber
        lift $ State.modify $ \s -> s
            { imports = newImports
            }

exprTy :: LHsBinds GhcTc -> Maybe Type
exprTy binds = listToMaybe
    [ argTy
    | FunBind{..} <- map unLoc (concatMap expand $ toList binds)
    , getOccText fun_id == "expr"
    , (_, [argTy]) <- [(splitTyConApp . mg_res_ty . mg_ext) fun_matches]
    ]

expand :: LHsBindLR id id -> [LHsBindLR id id]
expand (unLoc -> AbsBinds{..}) = toList abs_binds
expand bind = [bind]

lineFilePath :: Int -> NormalizedFilePath
lineFilePath i = toNormalizedFilePath' $ "Line" <> show i <> ".daml"

lineModuleName :: Int -> String
lineModuleName i = "Line" <> show i

-- | Possible ways to render a module. We take the first one that typechecks
data ModuleRenderings
    = BindingRendering ParsedSource -- ^ x <- e with e :: Script a for some a
    | BodyRenderings
        { unitScript :: ParsedSource
          -- ^ e :: Script (). Here we do not print the result.
        , printableScript :: ParsedSource
          -- ^ e :: Script a with for some a that is an instance of Show. Here
          -- we print the result.
        , arbitraryScript :: ParsedSource
          -- ^ e :: Script a for some a that may not be an instance of Show.
        , purePrintableExpr :: ParsedSource
          -- ^ e :: a for some a that is an instance of Show. Here we
          -- print the result. Note that we do not support
          -- non-printable pure expressions since there is no
          -- reason to run them.
        , pureArbitraryExpr :: ParsedSource
          -- ^ e :: a for some a that may not be an instance of Show.
        }

moduleImports
    :: DynFlags
    -> Imports
    -> [String]
moduleImports dflags imports =
    "import Daml.Script -- implicit"
    : map renderImport (importToList imports)
  where
    renderImport imp = showSDoc dflags (ppr imp)

renderModule
    :: Imports
    -> Int
    -> [(LPat GhcPs, Type)]
    -> SupportedStatement
    -> ModuleRenderings
renderModule imports line binds stmt = case stmt of
    BindStatement pat expr ->
        BindingRendering
          (buildExprModule file imports binds
             (noLoc $ HsWildCardTy noExt)
             (scriptStmt (Just pat) expr returnAp))
    BodyStatement expr ->
        BodyRenderings
          { unitScript =
              buildExprModule file imports binds
                (noLoc $ HsTyVar noExt NotPromoted (noLoc $ getRdrName unitTyCon))
                (scriptStmt Nothing expr returnAp)
          , printableScript =
              buildExprModule file imports binds
                 (noLoc $ HsTyVar noExt NotPromoted (noLoc $ Unqual $ mkTcOcc "Text"))
                 (scriptStmt Nothing expr returnShowAp)
          , arbitraryScript =
              buildExprModule file imports binds
                (noLoc $ HsWildCardTy noExt)
                (scriptStmt Nothing expr returnAp)
          , purePrintableExpr =
              buildExprModule file imports binds
                (noLoc $ HsTyVar noExt NotPromoted (noLoc $ Unqual $ mkTcOcc "Text"))
                (returnShowAp expr)
          , pureArbitraryExpr =
              buildExprModule file imports binds
                (noLoc $ HsWildCardTy noExt)
                (returnAp $ noLoc $ HsPar noExt expr)
          }
    LetStatement binding ->
        let retExpr = case binding of
                FunBinding f _ -> noLoc $ HsVar noExt f
                PatBinding pat _ -> toTupleExpr pat
            expr = noLoc $ HsDo noExt DoExpr $ noLoc
              [ noLoc $ LetStmt noExt $ toLocalBinds binding
              , noLoc $ LastStmt noExt (returnAp retExpr) False noSyntaxExpr
              ]
        in BindingRendering
             (buildExprModule file imports binds (noLoc $ HsWildCardTy noExt) expr)

  where
        file = lineModuleName line
        -- build a script statement using the given wrapper (either `return` or `show`)
        -- to wrap the final result.
        scriptStmt :: Maybe (LPat GhcPs) -> LHsExpr GhcPs -> (LHsExpr GhcPs -> LHsExpr GhcPs) -> LHsExpr GhcPs
        scriptStmt mbPat expr wrapper =
          let pat = fromMaybe (noLoc $ VarPat noExt $ noLoc $ mkRdrUnqual $ mkVarOcc "result") mbPat
          in noLoc $ HsDo noExt DoExpr $ noLoc
            [ noLoc $ BindStmt noExt pat expr noSyntaxExpr noSyntaxExpr
            , noLoc $ LastStmt noExt (wrapper $ toTupleExpr pat) False noSyntaxExpr
            ]
        returnAp :: LHsExpr GhcPs -> LHsExpr GhcPs
        returnAp = noLoc . HsApp noExt returnExpr
        returnExpr = noLoc $ HsVar noExt (noLoc $ mkRdrUnqual $ mkVarOcc "return")
        returnShowAp x =
            returnAp $
            noLoc $ HsPar noExt $
            noLoc $ HsApp noExt showExpr $
            noLoc $ HsPar noExt x
        showExpr = noLoc $ HsVar noExt (noLoc $ mkRdrUnqual $ mkVarOcc "show")

buildExprModule :: String -> Imports -> [(LPat GhcPs, Type)] -> LHsType GhcPs -> LHsExpr GhcPs -> GenLocated SrcSpan (HsModule GhcPs)
buildExprModule file imports binds ty expr = buildModule file imports
  [ noLoc $
    SigD noExt $
    TypeSig noExt [noLoc (Unqual $ mkVarOcc "expr")] $
    mkLHsSigWcType $
    let resTy =
          mkHsAppTy
           (noLoc $ HsTyVar noExt NotPromoted (noLoc $ Unqual $ mkTcOcc "Script"))
           ty
    in foldr (\(_, arg) acc -> noLoc $ HsFunTy noExt (noLoc $ XHsType $ NHsCoreTy arg) acc) resTy binds
  , noLoc $
    ValD noExt $ FunBind
      noExt
      (noLoc (Unqual $ mkVarOcc "expr"))
      (mkMatchGroup FromSource [mkSimpleMatch (mkPrefixFunRhs exprName) (map fst binds) expr])
      idHsWrapper
      []
  ]
  where
    exprName :: Located RdrName
    exprName = noLoc (Unqual $ mkVarOcc "expr")

buildModule :: String -> Imports -> [LHsDecl GhcPs] -> GenLocated SrcSpan (HsModule GhcPs)
buildModule file imports decls = L dummySrcSpan HsModule
  { hsmodName = Just (noLoc (mkModuleName file))
  , hsmodExports = Nothing
  , hsmodImports = Preprocessor.onImports $
      noLoc (simpleImportDecl (mkModuleName "Daml.Script")) :
      map noLoc (concat (Map.elems $ getImports imports))
  , hsmodDecls = decls
  , hsmodDeprecMessage = Nothing
  , hsmodHaddockModHeader = Nothing
  }
  where
      -- For some reason GHC requires a RealSrcSpan for typechecking so we make up one.
      dummySrcSpan = RealSrcSpan $ mkRealSrcSpan (mkRealSrcLoc (mkFastString (file <> ".daml")) 0 1) (mkRealSrcLoc (mkFastString file) 100000 1)

toParsedModule :: DynFlags -> GenLocated SrcSpan (HsModule GhcPs) -> ParsedModule
toParsedModule dflags source = ParsedModule
  { pm_mod_summary = ModSummary
      { ms_mod = mkModule mainUnitId (unLoc $ fromJust $ hsmodName $ unLoc source)
      , ms_hsc_src = HsSrcFile
      , ms_obj_date = Nothing
      , ms_iface_date = Nothing
      , ms_hie_date = Nothing
      , ms_srcimps = []
      , ms_textual_imps = []
      , ms_parsed_mod = Nothing
      , ms_hspp_opts =
          flip xopt_set PartialTypeSignatures $
          foldl'
            wopt_unset
            dflags
            [ Opt_WarnUnusedImports
            , Opt_WarnPartialTypeSignatures
            ]
      , ms_hspp_buf = Nothing
      -- These fields are not required for our usage
      -- so rather than making up values we set them to bottom
      -- making sure we error out if that changes.
      , ms_location = ModLocation
          { ml_hs_file = Nothing
          , ml_hi_file = error "ml_hi_file is not required"
          , ml_hie_file = error "ml_hie_file is not required"
          , ml_obj_file = error "ml_obj_file is not required"
          }
      , ms_hs_date = error "hs date is not required"
      , ms_hspp_file = error "hspp file is not required"
      }
  , pm_parsed_source = source
  , pm_extra_src_files = []
  , pm_annotations = (Map.empty, Map.empty)
  }

instance Applicative m => Apply (Repl.HaskelineT m) where
    liftF2 = liftA2

instance Monad m => Bind (Repl.HaskelineT m) where
    (>>-) = (>>=)


instance Semigroup Outputable.SDoc where
    (<>) = (Outputable.<>)

instance Monoid Outputable.SDoc where
    mempty = ""
