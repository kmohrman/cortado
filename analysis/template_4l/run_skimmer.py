import os
import socket
from coffea import processor
from coffea.nanoevents import NanoAODSchema

import skimmer_processor as analysis_processor

NanoAODSchema.warn_missing_crossrefs = False

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


if __name__ == '__main__':

    fpath = "/home/k.mohrman/coffea_dir/ewkcoffea_skimming_branch/test_with_virt_arrs/cortado/analysis/template_4l/output_2.root"

    # Tmp hard code arguments
    samplesdict = {'UL17_WWZJetsTo4L2Nu_forCI': {'xsec': 0.002067, 'year': '2017', 'treeName': 'Events', 'histAxisName': 'UL17_WWZJetsTo4L2Nu', 'options': '', 'WCnames': [], 'files': [fpath], 'nEvents': 112603, 'nGenEvents': 681198, 'nSumOfWeights': 1410.196684038, 'isData': False, 'path': '/home/users/mdittric/public_html/for_ci/for_wwz/WWZJetsTo4L2Nu_4F_TuneCP5_13TeV-amcatnlo-pythia8_RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v2_NANOAODSIM_WWZ_MC_2024_0811/', 'nSumOfLheWeights': [1493.9196748783966, 1468.0248329944698, 1446.9199483633354, 1434.4723716873198, 1390.039697813769, 1385.2237857914258, 1362.2975223263684, 1342.9391390344656], 'redirector': ''}}
    flist = {'UL17_WWZJetsTo4L2Nu_forCI': [fpath]}
    treename = "Events"
    chunksize = 100000
    nchunks=None
    nworkers=8


    # Check that if on UF login node, we're using TaskVine
    hostname = socket.gethostname()
    if "login" in hostname:
        # We are on a UF login node, better be using TaskVine
        # Note if this ends up catching more than UF, can also check for "login"&"ufhpc" in name
        if (executor != "taskvine"):
            raise Exception(f"\nError: We seem to be on a UF login node ({hostname}). If running from here, need to run with TaskVine.")


    #executor = "iterative"
    executor = "futures"
    #executor = "taskvine"

    #events = NanoEventsFactory.from_root({filename: "Events"}, mode="eager").events()
    #events = NanoEventsFactory.from_root({filename: "Events"}, mode="virtual").events()

    processor_instance = analysis_processor.AnalysisProcessor(samplesdict)

    if executor == "iterative":
        exec_instance = processor.IterativeExecutor()
        runner = processor.Runner(exec_instance, schema=NanoAODSchema, chunksize=chunksize, maxchunks=nchunks)
    elif executor == "futures":
        exec_instance = processor.FuturesExecutor(workers=nworkers)
        runner = processor.Runner(exec_instance, schema=NanoAODSchema, chunksize=chunksize, maxchunks=nchunks)
    elif executor == "taskvine":
        try:
            #executor = processor.TaskVineExecutor(**executor_args)
            executor = processor.TaskVineExecutor(**TASKVINE_ARGS)
        except AttributeError:
            raise RuntimeError("TaskVineExecutor not available.")
        runner = processor.Runner(
            executor,
            schema=NanoAODSchema,
            chunksize=chunksize,
            maxchunks=nchunks,
            skipbadfiles=True,
            xrootdtimeout=300,
        )


    output = runner(flist, processor_instance, treename)

    print("\nDone!")
