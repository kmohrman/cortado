from coffea import processor
from coffea.nanoevents import NanoAODSchema

import skimmer_processor as analysis_processor

if __name__ == '__main__':

    # Tmp hard code arguments
    samplesdict = {'UL17_WWZJetsTo4L2Nu_forCI': {'xsec': 0.002067, 'year': '2017', 'treeName': 'Events', 'histAxisName': 'UL17_WWZJetsTo4L2Nu', 'options': '', 'WCnames': [], 'files': ['output_2.root'], 'nEvents': 112603, 'nGenEvents': 681198, 'nSumOfWeights': 1410.196684038, 'isData': False, 'path': '/home/users/mdittric/public_html/for_ci/for_wwz/WWZJetsTo4L2Nu_4F_TuneCP5_13TeV-amcatnlo-pythia8_RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v2_NANOAODSIM_WWZ_MC_2024_0811/', 'nSumOfLheWeights': [1493.9196748783966, 1468.0248329944698, 1446.9199483633354, 1434.4723716873198, 1390.039697813769, 1385.2237857914258, 1362.2975223263684, 1342.9391390344656], 'redirector': ''}}
    flist = {'UL17_WWZJetsTo4L2Nu_forCI': ['output_2.root']}
    treename = "Events"
    executor = "iterative"
    chunksize = 100000
    nchunks=None

    #events = NanoEventsFactory.from_root({filename: "Events"}, mode="eager").events()
    #events = NanoEventsFactory.from_root({filename: "Events"}, mode="virtual").events()

    processor_instance = analysis_processor.AnalysisProcessor(samplesdict)

    exec_instance = processor.IterativeExecutor()
    runner = processor.Runner(exec_instance, schema=NanoAODSchema, chunksize=chunksize, maxchunks=nchunks)

    output = runner(flist, processor_instance, treename)





