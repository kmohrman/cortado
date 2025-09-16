import uproot
import awkward as ak
from coffea import processor
from coffea.nanoevents import NanoAODSchema

# From https://github.com/scikit-hep/coffea/discussions/735#discussioncomment-9646917
def is_rootcompat(a):
    """Is it a flat or 1-d jagged array?"""
    t = ak.type(a)
    if isinstance(t, ak.types.ArrayType):
        if isinstance(t.content, ak.types.NumpyType):
            return True
        if isinstance(t.content, ak.types.ListType) and isinstance(t.content.content, ak.types.NumpyType):
            return True
    return False


# From https://github.com/scikit-hep/coffea/discussions/735#discussioncomment-9646917
def uproot_writeable(events):
    """Restrict to columns that uproot can write compactly"""
    out = {}
    for bname in events.fields:
        if events[bname].fields:
            out[bname] = ak.zip({n: ak.to_packed(ak.without_parameters(events[bname][n])) for n in events[bname].fields if is_rootcompat(events[bname][n])})
        else:
            out[bname] = ak.to_packed(ak.without_parameters(events[bname]))
    return out


# Some placeholder selection
def make_skimmed_events(events):
    ele = events.Electron
    muo = events.Muon
    nlep = ak.num(ele) + ak.num(muo)
    mask = nlep >= 1
    return events[mask]


# Example processor
class AnalysisProcessor(processor.ProcessorABC):

    def __init__(self, samples):
        pass

    def process(self, events):

        fpath_out = "test_output.root"

        # Perform the skim
        out_events = make_skimmed_events(events)

        # Write out the events
        with uproot.recreate(fpath_out) as fout:
            fout["Events"] = uproot_writeable(out_events)

        return {}

    def postprocess(self, accumulator):
        pass


def main():

    fdict = {'test': ['https://raw.githubusercontent.com/CoffeaTeam/coffea/master/tests/samples/nano_dy.root']}

    processor_instance = AnalysisProcessor(fdict)
    exec_instance = processor.IterativeExecutor()
    runner = processor.Runner(exec_instance, schema=NanoAODSchema)

    output = runner(fdict, processor_instance, "Events")

main()

