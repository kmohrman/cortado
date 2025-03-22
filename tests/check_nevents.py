import awkward as ak
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

def test_nevents():

    fname = "analysis/template_4l/skimtest/for_ci/skimmed-part0.root"

    events = NanoEventsFactory.from_root(
        {fname: "tree"},
        schemaclass=NanoAODSchema,
    ).events()

    # Surely there's a better way to get nevents
    # But let's just check len of met
    met = events.PuppiMET
    nevents = len(met.pt.compute())

    print(f"Number of events in skimmed sample: {nevents}")

    assert (nevents == 42576)
