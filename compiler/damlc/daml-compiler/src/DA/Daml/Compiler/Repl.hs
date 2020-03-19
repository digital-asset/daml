-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TypeFamilies #-}
module DA.Daml.Compiler.Repl (runRepl) where

import BasicTypes (Boxity(..))
import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Exception hiding (TypeError)
import Control.Monad
import Control.Monad.Except
import Control.Monad.Trans.Maybe
import qualified DA.Daml.LF.Ast as LF
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Daml.LF.Reader (readDalfs, Dalfs(..))
import qualified DA.Daml.LF.ReplClient as ReplClient
import DA.Daml.LFConversion.UtilGHC
import DA.Daml.Options.Types
import Data.Bifunctor (first)
import qualified Data.ByteString.Lazy as BSL
import Data.Foldable
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
import System.Exit
import System.IO.Error
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

runRepl :: Options -> FilePath -> ReplClient.Handle -> IdeState -> IO ()
runRepl opts mainDar replClient ideState = do
    Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile mainDar
    (_, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))
    let moduleNames = map LF.moduleName (NM.elems (LF.packageModules pkg))
    Just pkgs <- runAction ideState (use GeneratePackageMap "Dummy.daml")
    Just stablePkgs <- runAction ideState (use GenerateStablePackages "Dummy.daml")
    for_ (toList pkgs <> toList stablePkgs) $ \pkg -> do
        r <- ReplClient.loadPackage replClient (LF.dalfPackageBytes pkg)
        case r of
            Left err -> do
                hPutStrLn stderr ("Package could not be loaded: " <> show err)
                exitFailure
            Right _ -> pure ()
    go moduleNames 0 []
  where
    handleLine
        :: [LF.ModuleName]
        -> [(LPat GhcPs, Type)]
        -> DynFlags
        -> String
        -> Int
        -> IO (Either Error (LPat GhcPs, Type))
    handleLine moduleNames binds dflags l i = runExceptT $ do
        stmt <- case parseStatement l dflags of
            POk _ lStmt -> pure (unLoc lStmt)
            PFailed _ _ errMsg -> throwError (ParseError errMsg)
        (bind, expr) <- maybe (throwError (UnsupportedStatement l)) pure (splitStmt stmt)
        liftIO $ writeFileUTF8 (fromNormalizedFilePath $ lineFilePath i)
            (renderModule dflags moduleNames i binds bind expr)
        -- Useful for debugging, probably best to put it behind a --debug flag
        -- rendered <- liftIO  $readFileUTF8 (fromNormalizedFilePath $ lineFilePath i)
        -- liftIO $ for_ (lines rendered) $ \line ->
        --      hPutStrLn stderr ("> " <> line)
        (lfMod, tmrModule -> tcMod) <-
            maybe (throwError TypeError) pure =<< liftIO (runAction ideState $ runMaybeT $
            (,) <$> useE GenerateDalf (lineFilePath i)
                <*> useE TypeCheck (lineFilePath i))
        -- Type of the statement so we can give it a type annotation
        -- and avoid incurring a typeclass constraint.
        stmtTy <- maybe (throwError TypeError) pure (exprTy $ tm_typechecked_source tcMod)
        scriptRes <- liftIO $ ReplClient.runScript replClient (optDamlLfVersion opts) lfMod
        case scriptRes of
            Right _ -> pure (bind, stmtTy)
            Left err -> throwError (ScriptError err)
    go :: [LF.ModuleName] -> Int -> [(LPat GhcPs, Type)] -> IO ()
    go moduleNames !i !binds = do
         putStr "daml> "
         hFlush stdout
         l <- catchJust (guard . isEOFError) getLine (const exitSuccess)
         dflags <-
             hsc_dflags . hscEnv <$>
             runAction ideState (use_ GhcSession $ lineFilePath i)
         r <- handleLine moduleNames binds dflags l i
         case r of
             Left err -> do
                 renderError dflags err
                 -- If we get an error we don’t increment i and we
                 -- do not get a new binding
                 go moduleNames i binds
             Right (pat, ty) -> do
                 let boundVars = mkOccSet (map occName (collectPatBinders pat))
                 go moduleNames
                    (i + 1 :: Int)
                    (map (first (shadowPat boundVars)) binds <> [(toTuplePat pat, ty)])

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
lineFilePath i = toNormalizedFilePath $ "Line" <> show i <> ".daml"

lineModuleName :: Int -> String
lineModuleName i = "Line" <> show i

renderModule :: DynFlags -> [LF.ModuleName] -> Int -> [(LPat GhcPs, Type)] -> LPat GhcPs -> LHsExpr GhcPs -> String
renderModule dflags imports line binds pat expr = unlines $
     [ "{-# OPTIONS_GHC -Wno-unused-imports -Wno-partial-type-signatures #-}"
     , "{-# LANGUAGE PartialTypeSignatures #-}"
     , "daml 1.2"
     , "module " <> lineModuleName line <> " where"
     , "import Daml.Script"
     ] <>
     map (\moduleName -> T.unpack $ "import " <> LF.moduleNameString moduleName) imports <>
     [ "expr : " <> concatMap (renderTy . snd) binds <> "Script _"
     ,  "expr " <> unwords (map (renderPat . fst) binds) <> " = "
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
  where renderPat pat = showSDoc dflags (ppr pat)
        renderTy ty = showSDoc dflags (ppr ty) <> " -> "
