import awkward as ak
import dask_awkward as dak 
import dask

def is_rootcompat(a):
    """Is it a flat or 1-d jagged array?"""
    t = dak.type(a)
    if isinstance(t, ak.types.NumpyType):
        return True
    if isinstance(t, ak.types.ListType) and isinstance(t.content, ak.types.NumpyType):
        return True

    return False


# From https://github.com/scikit-hep/coffea/discussions/1100
def uproot_writeable(events):
    """Restrict to columns that uproot can write compactly"""
    out_event = events[list(x for x in events.fields if not events[x].fields)]
    for bname in events.fields:
        if events[bname].fields:
            out_event[bname] = ak.zip(
                {
                    n: ak.without_parameters(events[bname][n])
                    for n in events[bname].fields
                    if is_rootcompat(events[bname][n])
                }
            )
    return out_event

# Some placeholder simple 4l selection
def make_skimmed_events(events):

    ele = events.Electron
    muo = events.Muon
    nlep = ak.num(ele) + ak.num(muo)
    mask = nlep >= 4
    print("e+m",nlep.compute())

    return events[mask]








