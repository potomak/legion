Restart example project (legion-discovery) with the new frontend code (run this
code from the legion-discovery directory):

rm -r ../legion/.stack-work/dist/ && \
  stack exec -- ghc-pkg unregister --force legion && \
  stack exec -- ghc-pkg unregister --force legion-discovery && \
  stack build && \
  stack exec legion-discovery -- -c config1.yml
