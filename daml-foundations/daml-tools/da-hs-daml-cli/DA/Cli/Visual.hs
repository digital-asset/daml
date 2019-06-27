-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ApplicativeDo       #-}

-- | Main entry-point of the DAML compiler
module DA.Cli.Visual
  ( execVisual
  ) where


import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Ast.World as AST
import DA.Daml.LF.Reader
import qualified Data.NameMap as NM
import qualified Data.Set as Set
import qualified DA.Pretty as DAP
import qualified DA.Daml.LF.Proto3.Archive as Archive
import qualified Codec.Archive.Zip as ZIPArchive
import qualified Data.ByteString.Lazy as BSL
import Text.Dot
import qualified Data.ByteString as B
import qualified Data.Map as M
import Data.Generics.Uniplate.Data
import Data.List.Extra
-- import Debug.Trace

data Action = ACreate (LF.Qualified LF.TypeConName)
            | AExercise (LF.Qualified LF.TypeConName) LF.ChoiceName deriving (Eq, Ord, Show )

startFromUpdate :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World -> LF.Update -> Set.Set Action
startFromUpdate seen world update = case update of
    LF.UPure _ e -> startFromExpr seen world e
    LF.UBind (LF.Binding _ e1) e2 -> startFromExpr seen world e1 `Set.union` startFromExpr seen world e2
    LF.UCreate tpl e -> Set.singleton (ACreate tpl) `Set.union` startFromExpr seen world e
    LF.UExercise tpl chc e1 e2 e3 -> Set.singleton (AExercise tpl chc) `Set.union` startFromExpr seen world e1 `Set.union` maybe Set.empty (startFromExpr seen world) e2 `Set.union` startFromExpr seen world e3
    LF.UFetch _ ctIdEx -> startFromExpr seen world ctIdEx
    LF.UGetTime -> Set.empty
    LF.UEmbedExpr _ upEx -> startFromExpr seen world upEx
    LF.ULookupByKey _ -> Set.empty
    LF.UFetchByKey _ -> Set.empty

startFromExpr :: Set.Set (LF.Qualified LF.ExprValName) -> LF.World  -> LF.Expr -> Set.Set Action
startFromExpr seen world e = case e of
    LF.EVar _ -> Set.empty
    LF.EVal ref ->  case LF.lookupValue ref world of
        Right LF.DefValue{..}
            | ref `Set.member` seen  -> Set.empty
            | otherwise -> startFromExpr (Set.insert ref seen)  world dvalBody
        Left _ -> error "This should not happen"
    LF.EUpdate upd -> startFromUpdate seen world upd
    LF.ETmApp (LF.ETyApp (LF.EVal (LF.Qualified _ (LF.ModuleName ["DA","Internal","Template"]) (LF.ExprValName "fetch"))) _) _ -> Set.empty
    LF.ETmApp (LF.ETyApp (LF.EVal (LF.Qualified _  (LF.ModuleName ["DA","Internal","Template"])  (LF.ExprValName "archive"))) _) _ -> Set.empty
    expr -> Set.unions $ map (startFromExpr seen world) $ children expr

startFromChoice :: LF.World -> LF.TemplateChoice -> Set.Set Action
startFromChoice world chc = startFromExpr Set.empty world (LF.chcUpdate chc)

data ChoiceAndAction = ChoiceAndAction { choice :: LF.TemplateChoice
                                        ,actions ::Set.Set Action
                                      }

data TemplateChoiceAction = TemplateChoiceAction { teamplate :: LF.Template
                                                  ,choiceAndAction :: [ChoiceAndAction] }

templatePossibleUpdates :: LF.World -> LF.Template -> [ChoiceAndAction]
templatePossibleUpdates world tpl = map (\c -> (ChoiceAndAction c (startFromChoice world c))  ) (NM.toList (LF.tplChoices tpl))

moduleAndTemplates :: LF.World -> LF.Module -> [TemplateChoiceAction]
moduleAndTemplates world mod = retTypess
    where
        templates = NM.toList $ LF.moduleTemplates mod
        retTypess = map (\t-> TemplateChoiceAction t (templatePossibleUpdates world t ) ) templates


dalfBytesToPakage :: BSL.ByteString -> (LF.PackageId, LF.Package)
dalfBytesToPakage bytes = case Archive.decodeArchive $ BSL.toStrict bytes of
    Right a -> a
    Left err -> error (show err)

darToWorld :: ManifestData -> LF.Package -> LF.World
darToWorld manifest pkg = AST.initWorldSelf pkgs pkg
    where
        pkgs = map dalfBytesToPakage (dalfsContent manifest)

type LookupTemplate = (LF.Qualified LF.TypeConName) -> LF.Template

lookupTemplateT :: LF.World -> (LF.Qualified LF.TypeConName) -> LF.Template
lookupTemplateT world qualTemplate = case AST.lookupTemplate qualTemplate world of
  Right tpl -> tpl
  Left _ -> error("Template lookup failed")

templateInAction ::  LookupTemplate  -> Action -> LF.Template
templateInAction lookupTemplate (ACreate  qtpl ) = lookupTemplate qtpl
templateInAction lookupTemplate (AExercise qtpl _ ) = lookupTemplate qtpl

srcLabel :: (LF.Template, [Action]) -> [(String, String)]
srcLabel (tc, _) = [("shape","none"), ("label",DAP.renderPretty $ LF.tplTypeCon tc) ]

templatePairs :: (LF.Template, Set.Set Action) -> (LF.Template , (LF.Template , [Action]))
templatePairs (tc, actions) = (tc , (tc,  Set.elems actions))

actionsForTemplate :: LookupTemplate -> (LF.Template, [Action]) -> [LF.Template]
actionsForTemplate lookupTemplate (_tplCon, actions) = map (templateInAction lookupTemplate) actions

errorOnLeft :: Show a => String -> Either a b -> IO b
errorOnLeft desc = \case
  Left err -> ioError $ userError $ unlines [ desc, show err ]
  Right x  -> return x

-- | 'netlistGraph' generates a simple graph from a netlist.
-- The default implementation does the edeges other way round. The change is on # 143
netlistGraph' :: (Ord a)
          => (b -> [(String,String)])   -- ^ Attributes for each node
          -> (b -> [a])                 -- ^ Out edges leaving each node
          -> [(a,b)]                    -- ^ The netlist
          -> Dot ()
netlistGraph' attrFn outFn assocs = do
    let nodes = Set.fromList [a | (a, _) <- assocs]
    let outs = Set.fromList [o | (_, b) <- assocs, o <- outFn b]
    nodeTab <- sequence
                [do nd <- node (attrFn b)
                    return (a, nd)
                | (a, b) <- assocs]
    otherTab <- sequence
               [do nd <- node []
                   return (o, nd)
                | o <- Set.toList outs, o `Set.notMember` nodes]
    let fm = M.fromList (nodeTab ++ otherTab)
    sequence_
        [(fm M.! dst) .->. (fm M.! src) | (dst, b) <- assocs,
        src <- outFn b]

execVisual :: FilePath -> Maybe FilePath -> IO ()
execVisual darFilePath dotFilePath = do
    darBytes <- B.readFile darFilePath
    let manifestData = manifestFromDar $ ZIPArchive.toArchive (BSL.fromStrict darBytes)
    (_, lfPkg) <- errorOnLeft "Cannot decode package" $ Archive.decodeArchive (BSL.toStrict (mainDalfContent manifestData) )
    let modules = NM.toList $ LF.packageModules lfPkg
        world = darToWorld manifestData lfPkg
        tplLookUp = lookupTemplateT world
        res = concatMap (moduleAndTemplates world) modules --  [(LF.Template, Set.Set Action)]
        actionEdges = map templatePairs res  -- (LF.Template , (LF.Template , [Action]))
        dotString = showDot $ netlistGraph' srcLabel (actionsForTemplate tplLookUp)  actionEdges
    case dotFilePath of
        Just outDotFile -> writeFile outDotFile dotString
        Nothing -> putStrLn dotString


