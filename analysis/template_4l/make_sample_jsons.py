import json
import subprocess

# Get a list of files for a dataset on DAS
def get_file_names_from_das(dataset_name):

    # Perform dasgoclient file query
    command_str = [f'dasgoclient --query="file dataset={dataset_name}"']
    subprocess_out = subprocess.run(command_str,capture_output=True,text=True,shell=True).stdout

    # Format the output from dasgoclient into a list of file names
    subprocess_out_lst = subprocess_out.split("\n")
    file_lst = []
    for fname in subprocess_out_lst:
        if fname != "": file_lst.append(fname)

    return file_lst


# Dump a list of filenames into a json
def dump_to_json(dataset_name,file_lst):

    # Name to call the output
    # Same as das datasete name except:
    #   - Drop any leading or training slashes
    #   - Replace all internal slashes with double underscores
    out_name = dataset_name
    if out_name.startswith("/"):
        out_name = out_name[1:]
    if out_name.endswith("/"):
        out_name = out_name[:-1]
    out_name = out_name.replace("/","__")

    # Dump to json
    out_dict = {"files" : file_lst}
    with open(f"{out_name}.json", "w") as f:
        json.dump(out_dict, f, indent=4)


def main():

    dataset_name = "/WWZJetsTo4L2Nu_4F_TuneCP5_13TeV-amcatnlo-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v2/NANOAODSIM"

    file_lst = get_file_names_from_das(dataset_name)
    dump_to_json(dataset_name,file_lst)



main()
