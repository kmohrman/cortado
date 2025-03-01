# cortado
This analysis repository contains tools for skimming NanoAOD with the `coffea` framework.

## Setup instructions

First, clone the repository and `cd` into the toplevel directory. 
```
git clone https://github.com/kmohrman/cortado.git
cd cortado
```
Next, create a `conda` environment and activate it. 
```
conda env create -f environment.yml
conda activate coffea-env
```
Now we can install the `cortado` package into our new conda environment. This command should be run from the toplevel `cortado` directory, i.e. the directory which contains the `setup.py` script. 
```
pip install -e .
```
Now all of the dependencies have been installed and the `cortado` repository is ready to be used. The next time you want to use it, all you have to do is to activate the environment via `conda activate cortado_env`. 

## Running
To run an example skimming workflow, navigate to the `template_4l` and run the example run script. 
```
cd analysis/template_4l
run_skimmer.py
```
If running with the `DaskVine` scheduler, an example command to submit a worker is below (example is relevant to running on `hipergator` at UF). It can be run in a separate terminal (but if running in a different terminal, remember to activate the conda environment there as well).
```
vine_submit_workers -T slurm --cores 2 --memory 4G -M coffea-vine-${USER} -p  "--account avery --qos avery --time 0:30:00" 1
```
The status of the projects can be viewed with the `vine_status` command, or by checking the [TaskVine monitoring page](https://ccl.cse.nd.edu/software/taskvine/status/). 
