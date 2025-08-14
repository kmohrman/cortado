import awkward as ak


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


# Some placeholder simple 4l selection
def make_skimmed_events(events):

    ele = events.Electron
    muo = events.Muon
    #nlep = ak.num(ele) + ak.num(muo)
    nlep = ak.num(ele)
    mask = nlep >= 4
    #print("e+m",nlep.compute())

    return events[mask]








