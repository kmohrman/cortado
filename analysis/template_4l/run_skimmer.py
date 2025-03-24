import argparse
import time
import socket
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

LST_OF_KNOWN_EXECUTORS = ["local","task_vine"]

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
    parser.add_argument('--executor','-x'  , default='local', help = 'Which executor to use', choices=LST_OF_KNOWN_EXECUTORS)
    parser.add_argument('--outlocation','-o'   , default='skimtest/', help = 'Location for the outputs')
    args = parser.parse_args()

    # Check that inputs are good
    # Check that if on UF login node, we're using WQ
    hostname = socket.gethostname()
    if "login" in hostname:
        # We are on a UF login node, better be using WQ
        # Note if this ends up catching more than UF, can also check for "login"&"ufhpc" in name
        if (args.executor == "local"):
            raise Exception(f"\nError: We seem to be on a UF login node ({hostname}). If running from here, do not run locally.")


    ############ Get list of files from the input jsons ############

    # Get the prefix and json names from the cfg file
    # This is quite brittle
    if args.sample_cfg_name.endswith(".cfg"):
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
    elif args.sample_cfg_name.endswith(".json"):
        # It seems we have been passed a single json instead of a config file with a list of jsons
        prefix=""
        json_lst = [args.sample_cfg_name]
    else:
        raise Exception("Unknown input type")

    # Build a sample dict with all info in the jsons
    samples_dict = {}
    for json_name in json_lst:
        with open(json_name) as jf:
            jf_loaded = json.load(jf)
            if jf_loaded["files"] == []: print(f"Empty file: {json_name}")
            samples_dict[json_name] = jf_loaded

    # Print some summary info about the files to be processed
    print(f"\nInformation about samples to be processed ({len(samples_dict)} total):")
    total_events = 0
    total_size = 0
    have_events_and_sizes = True
    for ds_name in samples_dict:
        if "nevents" in samples_dict[ds_name] and "size" in samples_dict[ds_name]:
            nevents = samples_dict[ds_name]["nevents"]
            size    = samples_dict[ds_name]["size"]
            total_events += nevents
            total_size   += size
            print(f"    Name: {ds_name} ({nevents} events)")
        else:
            print(f"    Name: {ds_name}")
            have_events_and_sizes = False
    if have_events_and_sizes:
        print(f"    Total events: {total_events}")
        print(f"    Total size: {total_size}\n")

    # Make the dataset object the processor wants
    dataset_dict = {}
    for json_path in samples_dict.keys():
        # Drop the .json from the end to get a name
        tag = json_path.split("/")[-1][:-5]
        # Prepend the prefix to the filenames and fill into the dataset dict
        dataset_dict[tag] = {}
        dataset_dict[tag]["files"] = {}
        for filename in samples_dict[json_path]["files"]:
            fullpath = prefix+filename
            dataset_dict[tag]["files"][fullpath] = "Events"

    #print(f"\nDataset dict:\n{dataset_dict}\n")



    ############ Set up DaskVine stuff ############

    if args.executor == "task_vine":
        m = DaskVine(
            [9123,9128],
            name=f"coffea-vine-{os.environ['USER']}",
            run_info_path="/blue/p.chang/k.mohrman/vine-run-info",
        )
        proxy = m.declare_file(f"/tmp/x509up_u{os.getuid()}", cache=True)



    ############ Run ############

    t_after_setup = time.time()

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
        skim_tools.make_skimmed_events,
        dataset_runnable,
        schemaclass=NanoAODSchema,
        uproot_options={"timeout": 180},
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
        print("\tRunning uproot_writeable")
        skimmed = skim_tools.uproot_writeable(
            skimmed,
        )
        t_dict["uproot_writeable"].append(time.time())

        # Reparititioning so that output has this many input partitions to on output
        print("\tRunning repartition")
        #skimmed = skimmed.repartition(n_to_one=1_000) # Comment for now, see https://github.com/dask-contrib/dask-awkward/issues/509
        t_dict["repartition"].append(time.time())

        # What does this do
        print("\tRunning dask_write")
        dask_write_out = uproot.dask_write(
            skimmed,
            destination=args.outlocation,
            prefix=f"{dataset}/skimmed",
            compute=False,
            tree_name="Events",
        )
        t_dict["dask_write"].append(time.time())

        # Call compute on the skimmed output
        if args.executor == "local":
            print("\tRunning dask.compute locally")
            dask.compute(
                dask_write_out,
            )
        if args.executor == "task_vine":
            print("\tRunning dask.compute with task_vine")
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


