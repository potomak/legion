module Main (main) where

import Distribution.PackageDescription
import Distribution.Simple (defaultMainWithHooks, hookedPrograms,
  simpleUserHooks, preBuild, preConf)
import Distribution.Simple.Program (simpleProgram, runProgram, Program,
  configureAllKnownPrograms, defaultProgramConfiguration, requireProgram)
import Distribution.Simple.Setup (fromFlagOrDefault, buildVerbosity,
  configVerbosity)
import Distribution.Simple.Utils (rawSystemExitCode)
import Distribution.Verbosity (normal)

main = defaultMainWithHooks simpleUserHooks {
    hookedPrograms = [elmProg],

    preConf = (\args flags -> do
        let verbosity = fromFlagOrDefault normal (configVerbosity flags)
        db <- configureAllKnownPrograms verbosity defaultProgramConfiguration
        _ <- requireProgram verbosity elmProg db
        preConf simpleUserHooks args flags
      ),

    preBuild = (\args flags -> do
        let verbosity = fromFlagOrDefault normal (buildVerbosity flags)
        db <- configureAllKnownPrograms verbosity defaultProgramConfiguration
        (elmMake, _) <- requireProgram verbosity elmProg db
        runProgram verbosity elmMake ["--yes"]
        preBuild simpleUserHooks args flags
      )
  }


{- | A description of the elm-make program.  -}
elmProg :: Program
elmProg = simpleProgram "elm-make"


