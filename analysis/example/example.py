from coffea.nanoevents import NanoAODSchema
from coffea.dataset_tools import preprocess, apply_to_fileset
import awkward as ak
import dask_awkward as dak 
import dask
import uproot

def is_rootcompat(a):
    """Is it a flat or 1-d jagged array?"""
    t = dak.type(a)
    if isinstance(t, ak.types.NumpyType):
        return True
    if isinstance(t, ak.types.ListType) and isinstance(t.content, ak.types.NumpyType):
        return True

    return False


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

def make_skimmed_events(events):
    # Place your selection logic here
    # Pass through
    return events


print("Running preprocessing")  # To obtain file splitting
dataset_runnable, _ = preprocess(
    {
        "dataset1": {
            "files": {
                f"10495AF7-2F9C-6440-BC72-4A29B6FEAEC7.root": "Events" for i in range(0, 100) # How ever many 
            }
        },
        "dataset2": {
            "files": {
                f"A6787F82-D6B8-424D-B35A-2A32FDB2837D.root": "Events" for i in range(0, 200) # Another data set
            }
        }
    },
    align_clusters=False,
    step_size=100_000,  # You may want to set this to something slightly smaller to avoid loading too much in memory
    files_per_batch=1,
    skip_bad_files=True,
    save_form=False,
)
print("Computing dask task graph")
skimmed_dict = apply_to_fileset(
    make_skimmed_events, dataset_runnable, schemaclass=NanoAODSchema
)


print("Executing task graph and saving")
for dataset, skimmed in skimmed_dict.items():
    skimmed = uproot_writeable(skimmed)
    skimmed = skimmed.repartition(
        n_to_one=1_000
    )  # Reparititioning so that output file contains ~100_000 eventspartition
    uproot.dask_write(
        skimmed,
        destination="skimtest/",
        prefix=f"{dataset}/skimmed",
    )
