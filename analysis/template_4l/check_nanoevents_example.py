from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

fname = "skimtest/for_ci/skimmed-part0.root"

events = NanoEventsFactory.from_root(
    #{fname: "Events"},
    {fname: "tree"},
    schemaclass=NanoAODSchema,
).events()

# For example, looking at MET
met = events.PuppiMET

# Print some info
print("")
#print("met:",met.pt.compute())
print("n events:",len(met.pt.compute()))
