import os
import socket
import argparse
import json
from coffea import processor
from coffea.nanoevents import NanoAODSchema

import skimmer_processor as analysis_processor

NanoAODSchema.warn_missing_crossrefs = False

LST_OF_KNOWN_EXECUTORS = ["iterative","futures","taskvine"]

TASKVINE_ARGS = {
    "manager_name": f"coffea-vine-{os.environ['USER']}",
    "port": 9123-9130,
    #"environment_file": remote_environment.get_environment(
    #    extra_pip_local={"topeft": ["topeft", "setup.py"]},
    #),
    "extra_input_files": ["analysis_processor.py"],
    "retries": 15,
    "compression": 0,
    "filepath": f'/tmp/{os.environ["USER"]}',
    "resource_monitor": "measure",
    "resources_mode": "auto",
    "treereduction": 10,
    "fast_terminate_workers": 0,
    "verbose": True,
    "print_stdout": False,
}

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
    parser.add_argument('--executor', '-x', default='iterative', help = 'Which executor to use', choices=LST_OF_KNOWN_EXECUTORS)
    parser.add_argument('--outlocation', '-o', default='skimtest/', help = 'Location for the outputs')
    args = parser.parse_args()

    # Just hard coding these for now..
    treename = "Events"
    chunksize = 100000
    nchunks=None
    #nworkers=8
    nworkers=32

    # Check that inputs are good
    # Check that if on UF login node, we're using TaskVine
    hostname = socket.gethostname()
    if "login" in hostname:
        # We are on a UF login node, better be using TaskVine
        # Note if this ends up catching more than UF, can also check for "login"&"ufhpc" in name
        if (args.executor != "taskvine"):
            raise Exception(f"\nError: We seem to be on a UF login node ({hostname}). If running from here, need to run with TaskVine.")


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
                if "#" in line:
                    # Ignore trailing comments, this file parsing is so brittle :(
                    json_lst.append(line.split("#")[0].strip())
                else:
                    json_lst.append(line)
    elif args.sample_cfg_name.endswith(".json"):
        # It seems we have been passed a single json instead of a config file with a list of jsons
        prefix=""
        json_lst = [args.sample_cfg_name]
    else:
        raise Exception("Unknown input type")

    # Build a sample dict with all info in the jsons
    samples_dict = {} # All of the info that we pass to the processor
    fileset_dict = {} # Just {dataset: [file, file], ...}
    for json_path in json_lst:
        tag = json_path.split("/")[-1][:-5]
        flst_with_prefix = [] # List of files with prefix appended

        # Load the json
        with open(json_path) as jf:
            jf_loaded = json.load(jf)
            if jf_loaded["files"] == []:
                print(f"Empty file list in this json: {json_path}")
                raise Exception("No files here, is this expected?")

            # Get the list of files with the prefix appended
            for filename in jf_loaded["files"]:
                flst_with_prefix.append(prefix+filename) # Brittle :(

            # Fill the fileset_dict
            fileset_dict[tag] = flst_with_prefix

            # Fill the samples_dict
            samples_dict[tag] = {}
            for key_name in jf_loaded:
                if key_name == "files":
                    samples_dict[tag][key_name] = flst_with_prefix
                else:
                    samples_dict[tag][key_name] = jf_loaded[key_name]

    #print(f"\nsamples dict:\n{samples_dict}\n")
    #print(f"\nfileset dict:\n{fileset_dict}\n")


    # Get and print some summary info about the files to be processed
    print(f"\nInformation about samples to be processed ({len(samples_dict)} total):")
    total_events = 0
    total_files = 0
    total_size = 0
    have_events_and_sizes = True
    # Get the info across the samples_dict
    for ds_name in samples_dict:
        nfiles = len(samples_dict[ds_name]["files"])
        total_files  += nfiles
        if "nevents" in samples_dict[ds_name] and "size" in samples_dict[ds_name]:
            # Only try to get this info if it's in the dict
            nevents = samples_dict[ds_name]["nevents"]
            size    = samples_dict[ds_name]["size"]
            total_events += nevents
            total_size   += size
            print(f"    Name: {ds_name} ({nevents} events)")
        else:
            print(f"    Name: {ds_name}")
            have_events_and_sizes = False
    # Print out totals
    print(f"    Total files: {total_files}")
    if have_events_and_sizes:
        print(f"    Total events: {total_events}")
        print(f"    Total size: {total_size}\n")



    ############ Running ############

    #events = NanoEventsFactory.from_root({filename: "Events"}, mode="eager").events()
    #events = NanoEventsFactory.from_root({filename: "Events"}, mode="virtual").events()

    processor_instance = analysis_processor.AnalysisProcessor(samples_dict)

    # Set up the runner
    if args.executor == "iterative":
        exec_instance = processor.IterativeExecutor()
        runner = processor.Runner(exec_instance, schema=NanoAODSchema, chunksize=chunksize, maxchunks=nchunks)
    elif args.executor == "futures":
        exec_instance = processor.FuturesExecutor(workers=nworkers)
        runner = processor.Runner(exec_instance, schema=NanoAODSchema, chunksize=chunksize, maxchunks=nchunks)
    elif args.executor == "taskvine":
        try:
            args.executor = processor.TaskVineExecutor(**TASKVINE_ARGS)
        except AttributeError:
            raise RuntimeError("TaskVineExecutor not available.")
        runner = processor.Runner(
            args.executor,
            schema=NanoAODSchema,
            chunksize=chunksize,
            maxchunks=nchunks,
            skipbadfiles=True,
            xrootdtimeout=300,
        )


    # Run the processor
    output = runner(fileset_dict, processor_instance, treename)
    print("\nDone!")

