import awkward as ak
import uproot
from coffea import processor

import cortado.modules.skim_tools as skim_tools

def is_rootcompat(a):
    """Is it a flat or 1-d jagged array?"""
    t = ak.type(a)
    if isinstance(t, ak.types.ArrayType):
        if isinstance(t.content, ak.types.NumpyType):
            return True
        if isinstance(t.content, ak.types.ListType) and isinstance(t.content.content, ak.types.NumpyType):
            return True
    return False


def uproot_writeable(events):
    """Restrict to columns that uproot can write compactly"""
    out = {}
    for bname in events.fields:
        if events[bname].fields:
            out[bname] = ak.zip({n: ak.to_packed(ak.without_parameters(events[bname][n])) for n in events[bname].fields if is_rootcompat(events[bname][n])})
        else:
            out[bname] = ak.to_packed(ak.without_parameters(events[bname]))
    return out

class AnalysisProcessor(processor.ProcessorABC):

    def __init__(self, samples):
        pass

    def process(self, events):

        print("hello world!")

        out_events = skim_tools.make_skimmed_events(events)
        with uproot.recreate("skimmedevents.root") as fout:
            fout["Events"] = uproot_writeable(out_events)

        return {}


    def postprocess(self, accumulator):
        pass
