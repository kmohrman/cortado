import os
import awkward as ak
import uproot
from coffea import processor

import cortado.modules.skim_tools as skim_tools


class AnalysisProcessor(processor.ProcessorABC):

    def __init__(self, samples, outdir):

        self._samples = samples
        self._outdir = outdir

    def process(self, events):

        # Create the output name
        json_name = events.metadata["dataset"]
        entrystart = events.metadata["entrystart"]
        fname_out = f"{json_name}_{entrystart}.root"
        fpath_out = os.path.join(self._outdir,fname_out)

        # Perform the skim
        out_events = skim_tools.make_skimmed_events(events)

        # Write out the events
        with uproot.recreate(fpath_out) as fout:
            fout["Events"] = skim_tools.uproot_writeable(out_events)

        return {}


    def postprocess(self, accumulator):
        pass
