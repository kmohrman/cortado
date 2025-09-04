from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

fname = "skimtest/for_ci_0.root"

events = NanoEventsFactory.from_root(
    {fname: "Events"},
    schemaclass=NanoAODSchema,
).events()

# For example, looking at MET
met = events.PuppiMET
print("n events 2:",len(met.pt))

