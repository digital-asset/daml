-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
module DA.Daml.Compiler.Repl
    ( newReplLogger
    , runRepl
    , ReplLogger(..)
    ) where

import BasicTypes (Boxity(..))
import Bag (bagToList, unitBag)
import Control.Applicative
import Control.Concurrent.Extra
import Control.Exception.Safe
import Control.Lens (toListOf)
import Control.Monad.Except
import Control.Monad.Extra
import qualified Control.Monad.State.Strict as State
import Control.Monad.Trans.Maybe
import DA.Daml.Compiler.Output (printDiagnostics)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import qualified DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.LFConversion.UtilGHC
import DA.Daml.Options.Types
import qualified DA.Daml.Preprocessor.Records as Preprocessor
import Data.Bifunctor (first)
import Data.Functor.Alt
import Data.Functor.Bind
import Data.Foldable
import Data.Generics.Uniplate.Data (descendBi)
import Data.Graph
import Data.IORef
import Data.List (intercalate)
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import Data.Semigroup (Last(..))
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules
import Development.IDE.Core.Shake
import Development.IDE.GHC.Util
import Development.IDE.LSP.Protocol
import Development.IDE.Types.Diagnostics
import Development.IDE.Types.Location
import ErrUtils
import GHC
import HsExpr (Stmt, StmtLR(..), LHsExpr)
import HsExtension (GhcPs, GhcTc)
import HsPat (Pat(..))
import HscTypes (HscEnv(..))
import Language.Haskell.GhclibParserEx.Parse
import Language.Haskell.LSP.Messages
import Lexer (ParseResult(..))
import Module (unitIdString)
import OccName (OccSet, occName, elemOccSet, mkOccSet, mkVarOcc)
import Outputable (ppr, showSDoc)
import qualified Outputable
import RdrName (mkRdrUnqual)
import SrcLoc (unLoc)
import qualified System.Console.Repline as Repl
import System.Exit
import System.IO.Extra
import TcEvidence (idHsWrapper)
import Type (splitTyConApp)

data Error
    = ParseError MsgDoc
    | UnsupportedStatement String -- ^ E.g., pattern on the LHS
    | UnknownModules [ModuleName]
    | TypeError -- ^ The actual error will be in the diagnostics
    | ScriptError ReplClient.BackendError

renderError :: DynFlags -> Error -> IO ()
renderError dflags err = case err of
    ParseError err ->
        putStrLn (showSDoc dflags err)
    (UnsupportedStatement str) ->
        putStrLn ("Unsupported statement: " <> str)
    (UnknownModules names) ->
        putStrLn ("Unknown modules: " <> intercalate ", " (map moduleNameString names))
    TypeError ->
        -- The error will be displayed via diagnostics.
        pure ()
    (ScriptError _err) ->
        -- The error will be displayed by the script runner.
        pure ()

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
    go SplicePat {} = error "DAML does not support Template Haskell"
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
toTupleExpr pat = noLoc $
    ExplicitTuple noExt [noLoc (Present noExt (noLoc $ HsVar noExt (noLoc v))) | v <- vars] Boxed
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

-- | Sort DALF packages in topological order.
-- I.e. if @a@ appears before @b@, then @b@ does not depend on @a@.
topologicalSort :: [LF.DalfPackage] -> [LF.DalfPackage]
topologicalSort lfPkgs = map toPkg $ topSort $ transposeG graph
  where
    (graph, fromVertex, _) = graphFromEdges
      [ (lfPkg, pkgId, deps)
      | lfPkg <- lfPkgs
      , let pkgId = LF.dalfPackageId lfPkg
      , let astPkg = LF.extPackagePkg (LF.dalfPackagePkg lfPkg)
      , let deps = [dep | LF.PRImport dep <- toListOf packageRefs astPkg]
      ]
    toPkg = (\(pkg, _, _) -> pkg) . fromVertex

data ReplState = ReplState
  { imports :: ![ImportDecl GhcPs]
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
-- Returns the list of modules in the specified import packages.
loadPackages :: [(LF.PackageName, Maybe LF.PackageVersion)] -> ReplClient.Handle -> IdeState -> IO [ImportDecl GhcPs]
loadPackages importPkgs replClient ideState = do
    -- Load packages
    Just (PackageMap pkgs) <- runAction ideState (use GeneratePackageMap "Dummy.daml")
    Just stablePkgs <- runAction ideState (useNoFile GenerateStablePackages)
    for_ (topologicalSort (toList pkgs <> toList stablePkgs)) $ \pkg -> do
        r <- ReplClient.loadPackage replClient (LF.dalfPackageBytes pkg)
        case r of
            Left err -> do
                hPutStrLn stderr ("Package could not be loaded: " <> show err)
                exitFailure
            Right _ -> pure ()
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
    pure
      [ simpleImportDecl . mkModuleName . T.unpack . LF.moduleNameString $ mod
      | pkg <- importLfPkgs
      , mod <- NM.names $ LF.packageModules pkg
      ]

data ReplLogger = ReplLogger
  { withReplLogger :: forall a. ([FileDiagnostic] -> IO ()) -> IO a -> IO a
  -- ^ Temporarily modify what happens to diagnostics
  , replEventLogger :: FromServerMessage -> IO ()
  -- ^ Logger to pass to `withDamlIdeState`
  }


newReplLogger :: IO ReplLogger
newReplLogger = do
    lock <- newLock
    diagsRef <- newIORef $ \diags -> printDiagnostics stdout diags
    let replEventLogger = \case
            EventFileDiagnostics fp diags -> do
                logger <- readIORef diagsRef
                logger $ map (toNormalizedFilePath' fp, ShowDiag,) diags
            _ -> pure ()
        withReplLogger :: ([FileDiagnostic] -> IO ()) -> IO a -> IO a
        withReplLogger logAct f =
            withLock lock $
            bracket
                (readIORef diagsRef <* atomicWriteIORef diagsRef logAct)
                (atomicWriteIORef diagsRef)
                (const f)
    pure ReplLogger{..}

runRepl
    :: [(LF.PackageName, Maybe LF.PackageVersion)]
    -> Options
    -> ReplClient.Handle
    -> ReplLogger
    -> IdeState
    -> IO ()
runRepl importPkgs opts replClient logger ideState = do
    imports <- loadPackages importPkgs replClient ideState
    let initReplState = ReplState
          { imports = imports
          , bindings = []
          , lineNumber = 0
          }
    -- TODO[AH] Use Repl.evalReplOpts once we're using repline >= 0.2.2
    let replM = Repl.evalRepl banner command options prefix tabComplete initialiser
          where
            banner = pure "daml> "
            command = replLine
            options = replOptions
            prefix = Just ':'
            tabComplete = Repl.Cursor $ \_ _ -> pure []
            initialiser = pure ()
    State.evalStateT replM initReplState
  where
    handleStmt
        :: DynFlags
        -> String
        -> Stmt GhcPs (LHsExpr GhcPs)
        -> ExceptT Error ReplM ()
    handleStmt dflags line stmt = do
        ReplState {imports, bindings, lineNumber} <- State.get
        supportedStmt <- maybe (throwError (UnsupportedStatement line)) pure (validateStmt stmt)
        let rendering = renderModule dflags imports lineNumber bindings supportedStmt
        (lfMod, tmrModule -> tcMod) <- printDelayedDiagnostics $ case rendering of
            BindingRendering t ->
                tryTypecheck lineNumber (T.pack t)
            BodyRenderings {..} ->
                withExceptT getLast
                $   withExceptT Last (tryTypecheck lineNumber (T.pack unitScript))
                <!> withExceptT Last (tryTypecheck lineNumber (T.pack printableScript))
                <!> withExceptT Last (tryTypecheck lineNumber (T.pack nonprintableScript))
                <!> withExceptT Last (tryTypecheck lineNumber (T.pack purePrintableExpr))
        -- Type of the statement so we can give it a type annotation
        -- and avoid incurring a typeclass constraint.
        stmtTy <- maybe (throwError TypeError) pure (exprTy $ tm_typechecked_source tcMod)
        -- If we get an error we don’t increment lineNumber and we
        -- do not get a new binding
        mbResult <- withExceptT ScriptError $ ExceptT $ liftIO $
            ReplClient.runScript replClient (optDamlLfVersion opts) lfMod
        liftIO $ whenJust mbResult T.putStrLn
        let boundVars = stmtBoundVars supportedStmt
            boundVars' = mkOccSet $ map occName boundVars
        State.put $! ReplState
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
    tryTypecheck :: Int -> T.Text -> ExceptT (Error, [[FileDiagnostic]]) ReplM (LF.Module, TcModuleResult)
    tryTypecheck lineNumber t = do
        liftIO $ setBufferModified ideState (lineFilePath lineNumber) $ Just t
        -- We need to temporarily suppress diagnostics since we use type errors
        -- to decide what to do. If a case succeeds we immediately print all diagnostics.
        -- If it fails, we return them and only print them once everything failed.
        diagsRef <- liftIO $ newIORef id
        let writeDiags diags = atomicModifyIORef diagsRef (\f -> (f . (diags:), ()))
        r <- liftIO $ withReplLogger logger writeDiags $ runAction ideState $ runMaybeT $
            (,) <$> useE GenerateDalf (lineFilePath lineNumber)
                <*> useE TypeCheck (lineFilePath lineNumber)
        diags <- liftIO $ ($ []) <$> readIORef diagsRef
        case r of
            Nothing -> throwError (TypeError, diags)
            Just r -> do
                liftIO $ mapM_ (printDiagnostics stdout) diags
                pure r
    handleImport
        :: DynFlags
        -> ImportDecl GhcPs
        -> ExceptT Error ReplM ()
    handleImport dflags imp = addImports dflags [imp]
    replLine :: String -> ReplM ()
    replLine line = do
        ReplState {lineNumber} <- State.get
        dflags <- liftIO $
            hsc_dflags . hscEnv <$>
            runAction ideState (use_ GhcSession $ lineFilePath lineNumber)
        r <- runExceptT $ do
            input <- ExceptT $ pure $ parseReplInput line dflags
            case input of
                ReplStatement stmt -> handleStmt dflags line stmt
                ReplImport imp -> handleImport dflags imp
        case r of
            Left err -> liftIO $ renderError dflags err
            Right () -> pure ()

    mkReplOption
        :: (DynFlags -> [String] -> ExceptT Error ReplM ())
        -> [String] -> ReplM ()
    mkReplOption option args = do
        ReplState {lineNumber} <- State.get
        dflags <- liftIO $
            hsc_dflags . hscEnv <$>
            runAction ideState (use_ GhcSession $ lineFilePath lineNumber)
        r <- runExceptT $ option dflags args
        case r of
            Left err -> liftIO $ renderError dflags err
            Right () -> pure ()
    replOptions :: [(String, [String] -> ReplM ())]
    replOptions =
      [ ("help", mkReplOption optHelp)
      , ("module", mkReplOption optModule)
      ]
    optHelp _dflags _args = liftIO $ T.putStrLn $ T.unlines
      [ " Commands available from the prompt:"
      , ""
      , "   <statement>                 evaluate/run <statement>"
      , "   :module [+/-] <mod> ...     add or remove the modules from the import list"
      ]
    optModule _dflags ("-" : modules) = do
        ReplState {imports} <- lift State.get
        -- TODO[AH] Use a more appropriate data structure to track imports.
        let unknown =
              [ removed
              | removed <- map mkModuleName modules
              , removed `notElem` map (unLoc . ideclName) imports
              ]
            newImports =
              [ simpleImportDecl imported
              | imported <- map (unLoc . ideclName) imports
              , imported `notElem` map mkModuleName modules
              ]
        unless (null unknown) $
            throwError $ UnknownModules unknown
        lift $ State.modify $ \s -> s { imports = newImports }
    optModule dflags ("+" : modules) = do
        let imports = map (simpleImportDecl . mkModuleName) modules
        addImports dflags imports
    optModule dflags modules = do
        let imports = map (simpleImportDecl . mkModuleName) modules
        addImports dflags imports

    addImports
        :: DynFlags
        -> [ImportDecl GhcPs]
        -> ExceptT Error ReplM ()
    addImports dflags additional = do
        ReplState {imports, lineNumber} <- State.get
        -- TODO[AH] Deduplicate imports.
        let newImports = additional ++ imports
        _ <- printDelayedDiagnostics $ tryTypecheck lineNumber $
            T.pack (unlines $ moduleHeader dflags newImports lineNumber)
        State.modify $ \s -> s { imports = newImports }

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
    = BindingRendering String -- ^ x <- e with e :: Script a for some a
    | BodyRenderings
        { unitScript :: String
          -- ^ e :: Script (). Here we do not print the result.
        , printableScript :: String
          -- ^ e :: Script a with for some a that is an instance of Show. Here
          -- we print the result.
        , nonprintableScript :: String
          -- ^ e :: Script a for some a that is not an instance of Show.
        , purePrintableExpr :: String
          -- ^ e :: a for some a that is an instance of Show. Here we
          -- print the result. Note that we do not support
          -- non-printable pure expressions since there is no
          -- reason to run them.
        }

moduleHeader
    :: DynFlags
    -> [ImportDecl GhcPs]
    -> Int
    -> [String]
moduleHeader dflags imports line =
    [ "{-# OPTIONS_GHC -Wno-unused-imports -Wno-partial-type-signatures #-}"
    , "{-# LANGUAGE PartialTypeSignatures #-}"
    , "module " <> lineModuleName line <> " where"
    ] <>
    ( "import Daml.Script"
    : map renderImport imports
    )
  where
    renderImport imp = showSDoc dflags (ppr imp)

renderModule
    :: DynFlags
    -> [ImportDecl GhcPs]
    -> Int
    -> [(LPat GhcPs, Type)]
    -> SupportedStatement
    -> ModuleRenderings
renderModule dflags imports line binds stmt = case stmt of
    BindStatement pat expr ->
        BindingRendering $ unlines $
            moduleHeader dflags imports line <>
            [ exprTy "Script _"
            , exprLhs
            , showSDoc dflags $ Outputable.nest 2 $ ppr (scriptStmt (Just pat) expr returnAp)
            ]
    BodyStatement expr ->
        BodyRenderings
          { unitScript = unlines $
              moduleHeader dflags imports line <>
              [ exprTy "Script ()"
              , exprLhs
              , showSDoc dflags $ Outputable.nest 2 $ ppr (scriptStmt Nothing expr returnAp)
              ]
          , printableScript = unlines $
              moduleHeader dflags imports line <>
              [ exprTy "Script Text"
              , exprLhs
              , showSDoc dflags $ Outputable.nest 2 $ ppr (scriptStmt Nothing expr returnShowAp)
              ]
          , nonprintableScript = unlines $
              moduleHeader dflags imports line <>
              [ exprTy "Script _"
              , exprLhs
              , showSDoc dflags $ Outputable.nest 2 $ ppr (scriptStmt Nothing expr returnAp)
              ]
          , purePrintableExpr = unlines $
              moduleHeader dflags imports line <>
              [ exprTy "Script Text"
              , exprLhs
              , showSDoc dflags $ Outputable.nest 2 $ ppr $
                returnShowAp expr
              ]
          }
    LetStatement binding ->
        let retExpr = case binding of
                FunBinding f _ -> noLoc $ HsVar noExt f
                PatBinding pat _ -> toTupleExpr pat
        in BindingRendering $ unlines $
          moduleHeader dflags imports line <>
          [ exprTy "Script _"
          , exprLhs
          , showSDoc dflags $ Outputable.nest 2 $ ppr $ HsDo noExt DoExpr $ noLoc
              [ noLoc $ LetStmt noExt $ toLocalBinds binding
              , noLoc $ LastStmt noExt (returnAp retExpr) False noSyntaxExpr
              ]
          ]
  where
        renderPat pat = showSDoc dflags (ppr pat)
        renderTy ty = "(" <> showSDoc dflags (ppr ty) <> ") -> "
        -- build a script statement using the given wrapper (either `return` or `show`)
        -- to wrap the final result.
        scriptStmt mbPat expr wrapper =
          let pat = fromMaybe (noLoc $ VarPat noExt $ noLoc $ mkRdrUnqual $ mkVarOcc "result") mbPat
          in HsDo noExt DoExpr $ noLoc
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
        exprLhs = "expr " <> unwords (map (renderPat . fst) binds) <> " = "
        exprTy res = "expr : " <> concatMap (renderTy . snd) binds <> res

instance Applicative m => Apply (Repl.HaskelineT m) where
    liftF2 = liftA2

instance Monad m => Bind (Repl.HaskelineT m) where
    (>>-) = (>>=)
