-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TypeFamilies #-}
module DA.Daml.Compiler.Repl (runRepl) where

import BasicTypes (Boxity(..))
import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Lens (toListOf)
import Control.Monad.Except
import qualified Control.Monad.State.Strict as State
import Control.Monad.Trans.Maybe
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.Optics (packageRefs)
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Daml.LF.Reader (readDalfs, Dalfs(..))
import qualified DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.LFConversion.UtilGHC
import DA.Daml.Options.Types
import qualified DA.Daml.Preprocessor.Records as Preprocessor
import Data.Bifunctor (first)
import qualified Data.ByteString.Lazy as BSL
import Data.Functor.Alt
import Data.Foldable
import Data.Generics.Uniplate.Data (descendBi)
import Data.Graph
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules
import Development.IDE.Core.Shake
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
import ErrUtils
import GHC
import HsExpr (Stmt, StmtLR(..), LHsExpr)
import HsExtension (GhcPs, GhcTc)
import HsPat (Pat(..))
import HscTypes (HscEnv(..))
import Language.Haskell.GhclibParserEx.Parse
import Lexer (ParseResult(..))
import OccName (occName, OccSet, elemOccSet, mkOccSet, mkVarOcc)
import Outputable (ppr, showSDoc)
import RdrName (mkRdrUnqual)
import SrcLoc (unLoc)
import qualified System.Console.Repline as Repl
import System.Exit
import System.IO.Extra
import Type

data Error
    = ParseError MsgDoc
    | UnsupportedStatement String -- ^ E.g., pattern on the LHS
    | TypeError -- ^ The actual error will be in the diagnostics
    | ScriptError ReplClient.BackendError

renderError :: DynFlags -> Error -> IO ()
renderError dflags err = case err of
    ParseError err ->
        putStrLn (showSDoc dflags err)
    (UnsupportedStatement str) ->
        putStrLn ("Unsupported statement: " <> str)
    TypeError ->
        -- ^ The error will be displayed via diagnostics.
        pure ()
    (ScriptError _err) ->
        -- ^ The error will be displayed by the script runner.
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

toTuplePat :: LPat GhcPs -> LPat GhcPs
toTuplePat pat = noLoc $
    TuplePat noExt [noLoc (VarPat noExt $ noLoc v) | v <- vars] Boxed
  where vars = collectPatBinders pat

toTupleExpr :: LPat GhcPs -> LHsExpr GhcPs
toTupleExpr pat = noLoc $
    ExplicitTuple noExt [noLoc (Present noExt (noLoc $ HsVar noExt (noLoc v))) | v <- vars] Boxed
  where vars = collectPatBinders pat

-- | Split a statement into the pattern and the body.
-- For unsupported statements we return `Nothing`.
splitStmt :: Stmt GhcPs (LHsExpr GhcPs) -> Maybe (LPat GhcPs, LHsExpr GhcPs)
splitStmt (BodyStmt _ expr _ _) = Just (noLoc $ WildPat noExt, expr)
splitStmt (BindStmt _ pat expr _ _) = Just (ParPat noExt pat, expr)
splitStmt _ = Nothing

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


runRepl :: Options -> FilePath -> ReplClient.Handle -> IdeState -> IO ()
runRepl opts mainDar replClient ideState = do
    Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile mainDar
    (_, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))
    let moduleNames = map LF.moduleName (NM.elems (LF.packageModules pkg))
    Just pkgs <- runAction ideState (use GeneratePackageMap "Dummy.daml")
    Just stablePkgs <- runAction ideState (use GenerateStablePackages "Dummy.daml")
    for_ (topologicalSort (toList pkgs <> toList stablePkgs)) $ \pkg -> do
        r <- ReplClient.loadPackage replClient (LF.dalfPackageBytes pkg)
        case r of
            Left err -> do
                hPutStrLn stderr ("Package could not be loaded: " <> show err)
                exitFailure
            Right _ -> pure ()
    let initReplState = ReplState
          { imports = map (simpleImportDecl . mkModuleName . T.unpack . LF.moduleNameString) moduleNames
          , bindings = []
          , lineNumber = 0
          }
    -- TODO[AH] Use Repl.evalReplOpts once we're using repline >= 0.2.2
    let replM = Repl.evalRepl banner command options prefix tabComplete initialiser
          where
            banner = pure "daml> "
            command = replLine
            options = []
            prefix = Nothing
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
        (bind, expr) <- maybe (throwError (UnsupportedStatement line)) pure (splitStmt stmt)
        liftIO $ setBufferModified ideState (lineFilePath lineNumber)
            $ Just $ T.pack (renderModule dflags imports lineNumber bindings bind expr)
        (lfMod, tmrModule -> tcMod) <-
            maybe (throwError TypeError) pure =<< liftIO (runAction ideState $ runMaybeT $
            (,) <$> useE GenerateDalf (lineFilePath lineNumber)
                <*> useE TypeCheck (lineFilePath lineNumber))
        -- Type of the statement so we can give it a type annotation
        -- and avoid incurring a typeclass constraint.
        stmtTy <- maybe (throwError TypeError) pure (exprTy $ tm_typechecked_source tcMod)
        -- If we get an error we don’t increment lineNumber and we
        -- do not get a new binding
        withExceptT ScriptError $ ExceptT $ liftIO $
            ReplClient.runScript replClient (optDamlLfVersion opts) lfMod
        let boundVars = mkOccSet (map occName (collectPatBinders bind))
        State.put $! ReplState
          { imports = imports
          , bindings = map (first (shadowPat boundVars)) bindings <> [(toTuplePat bind, stmtTy)]
          , lineNumber = lineNumber + 1
          }
    handleImport
        :: DynFlags
        -> ImportDecl GhcPs
        -> ExceptT Error ReplM ()
    handleImport dflags imp = do
        ReplState {imports, lineNumber} <- State.get
        -- TODO[AH] Deduplicate imports.
        let newImports = imp : imports
        -- TODO[AH] Factor out the module render and typecheck step.
        liftIO $ setBufferModified ideState (lineFilePath lineNumber)
            $ Just $ T.pack (renderModule dflags newImports lineNumber [] (WildPat noExt)
                (noLoc $ HsApp noExt
                    (noLoc $ HsVar noExt $ noLoc $ mkRdrUnqual $ mkVarOcc "return")
                    (noLoc $ ExplicitTuple noExt [] Boxed)))
        _ <- maybe (throwError TypeError) pure =<< liftIO (runAction ideState $ runMaybeT $
            (,) <$> useE GenerateDalf (lineFilePath lineNumber)
                <*> useE TypeCheck (lineFilePath lineNumber))
        State.modify $ \s -> s { imports = newImports }
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

renderModule :: DynFlags -> [ImportDecl GhcPs] -> Int -> [(LPat GhcPs, Type)] -> LPat GhcPs -> LHsExpr GhcPs -> String
renderModule dflags imports line binds pat expr = unlines $
     [ "{-# OPTIONS_GHC -Wno-unused-imports -Wno-partial-type-signatures #-}"
     , "{-# LANGUAGE PartialTypeSignatures #-}"
     , "daml 1.2"
     , "module " <> lineModuleName line <> " where"
     , "import Daml.Script"
     ] <>
     map renderImport imports <>
     [ "expr : " <> concatMap (renderTy . snd) binds <> "Script _"
     , "expr " <> unwords (map (renderPat . fst) binds) <> " = "
     ] <>
     let stmt = HsDo noExt DoExpr $ noLoc
             [ noLoc $ BindStmt noExt pat expr noSyntaxExpr noSyntaxExpr
             , noLoc $ LastStmt noExt (noLoc $ HsApp noExt returnExpr tupleExpr) False noSyntaxExpr
             ]
         returnExpr = noLoc $ HsVar noExt (noLoc $ mkRdrUnqual $ mkVarOcc "return")
         tupleExpr = toTupleExpr pat
     in -- indent by two spaces.
        -- we might just want to construct the whole function using the Haskell AST
        -- so the Haskell pretty printer takes care of this stuff.
        map ("  " <> ) $ lines $ prettyPrint stmt
  where renderImport imp = showSDoc dflags (ppr imp)
        renderPat pat = showSDoc dflags (ppr pat)
        renderTy ty = showSDoc dflags (ppr ty) <> " -> "
