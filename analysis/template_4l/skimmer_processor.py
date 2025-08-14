import awkward as ak
import uproot
from coffea import processor

import cortado.modules.skim_tools as skim_tools


class AnalysisProcessor(processor.ProcessorABC):

    def __init__(self, samples):
        pass

    def process(self, events):

        out_events = skim_tools.make_skimmed_events(events)
        with uproot.recreate("skimmedevents.root") as fout:
            fout["Events"] = skim_tools.uproot_writeable(out_events)

        return {}


    def postprocess(self, accumulator):
        pass
