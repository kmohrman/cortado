import argparse
import time
import yaml
import json
import os
import uproot
import dask
from ndcctools.taskvine import DaskVine
from coffea.nanoevents import NanoAODSchema
from coffea.dataset_tools import preprocess, apply_to_fileset

import cortado.modules.skim_tools as skim_tools

t_start = time.time()

NanoAODSchema.warn_missing_crossrefs = False

# Read and input file and return the lines
def read_file(filename):
    with open(filename) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    return content

# Print timing info in s and m
def pretty_print_time(t0,t1,tag,indent=" "*4):
    dt = t1-t0
    print(f"{indent}{tag}: {round(dt,3)}s  ({round(dt/60,3)}m)")


if __name__ == '__main__':


    ############ Parse input args ############

    parser = argparse.ArgumentParser()
    parser.add_argument("sample_cfg_name", help = "The name of the cfg file")
    args = parser.parse_args()


    ############ Get list of files from the input jsons ############

    # Get the prefix and json names from the cfg file
    prefix = ""
    json_lst = []
    lines = read_file(args.sample_cfg_name)
    for line in lines:
        if line.startswith("#"): continue
        if line == "": continue
        elif line.startswith("prefix:"):
            prefix = line.split()[1]
        else:
            json_lst.append(line)

    # Build a sample dict with all info in the jsons
    samples_dict = {}
    for json_name in json_lst:
        with open(json_name) as jf:
            samples_dict[json_name] = json.load(jf)

    # Make the dataset object the processor wants
    #print(samples_dict)
    dataset_dict = {}
    for json_path in samples_dict.keys():
        tag = json_path.split("/")[-1][:-5]
        dataset_dict[tag] = {}
        dataset_dict[tag]["files"] = {}
        for filename in samples_dict[json_path]["files"]:
            fullpath = prefix+filename
            dataset_dict[tag]["files"][fullpath] = "Events"
    print(f"\nDataset dict:\n{dataset_dict}\n")


    ############ Set up DaskVine stuff ############

    m = DaskVine(
        [9123,9128],
        name=f"coffea-vine-{os.environ['USER']}",
        run_info_path="/blue/p.chang/k.mohrman/vine-run-info",
    )
    proxy = m.declare_file(f"/tmp/x509up_u{os.getuid()}", cache=True)

    t_after_setup = time.time()



    ############ Run ############

    # This section is mainly copied from: https://github.com/scikit-hep/coffea/discussions/1100

    # Run preprocess
    print("\nRunning preprocessing..")  # To obtain file splitting
    dataset_runnable, _ = preprocess(
        dataset_dict,
        align_clusters=False,
        step_size=100_000,  # You may want to set this to something slightly smaller to avoid loading too much in memory
        files_per_batch=1,
        skip_bad_files=True,
        save_form=False,
    )
    t_after_preprocess = time.time()


    # Run apply_to_fileset
    print("\nComputing dask task graph..")
    skimmed_dict = apply_to_fileset(
        skim_tools.make_skimmed_events, dataset_runnable, schemaclass=NanoAODSchema
    )
    t_after_applytofileset = time.time()
    print(f"\nSkimmed dict:\n{skimmed_dict}\n")


    # Loop over datasets and execute task graph and save
    print("Executing task graph and saving")
    dataset_counter = 0
    t_dict = {"loop_start":[], "uproot_writeable":[], "repartition":[], "dask_write":[], "dask.compute":[]}
    for dataset, skimmed in skimmed_dict.items():
        t_dict["loop_start"].append(time.time())
        dataset_counter = dataset_counter + 1
        print(f"Name of dataset {dataset_counter}: {dataset}")

        # What does this do
        skimmed = skim_tools.uproot_writeable(
            skimmed,
        )
        t_dict["uproot_writeable"].append(time.time())

        # Reparititioning so that output has this many input partitions to on output
        skimmed = skimmed.repartition(n_to_one=1_000)
        t_dict["repartition"].append(time.time())

        # What does this do
        dask_write_out = uproot.dask_write(
            skimmed,
            destination="skimtest/",
            prefix=f"{dataset}/skimmed",
            compute=False,
        )
        t_dict["dask_write"].append(time.time())

        # Call compute on the skimmed output
        dask.compute(
            dask_write_out,
            scheduler=m.get,
            lazy_transfers=True,
            extra_files={proxy: "proxy.pem"},
            env_vars={"X509_USER_PROXY": "proxy.pem"},
        )
        t_dict["dask.compute"].append(time.time())

    t_end = time.time()



    ############ Print timing info ############

    print("\nTiming info:")
    pretty_print_time(t_start,                t_after_setup,          "Time to setup")
    pretty_print_time(t_after_setup,          t_after_preprocess,     "Time for preprocess")
    pretty_print_time(t_after_preprocess,     t_after_applytofileset, "Time for apply_to_fileset")
    for i in range(len(skimmed_dict)):
        pretty_print_time(t_dict["loop_start"][i],       t_dict["uproot_writeable"][i], f"Dataset {i}: time for uproot_writeable", "\t")
        pretty_print_time(t_dict["uproot_writeable"][i], t_dict["repartition"][i],      f"Dataset {i}: time for repartition", "\t")
        pretty_print_time(t_dict["repartition"][i],      t_dict["dask_write"][i],       f"Dataset {i}: time for dask_write", "\t")
        pretty_print_time(t_dict["dask_write"][i],       t_dict["dask.compute"][i],     f"Dataset {i}: time for dask.compute", "\t")
    pretty_print_time(t_after_applytofileset, t_end, "Time for the full run loop")
    print("\nDone!\n")


