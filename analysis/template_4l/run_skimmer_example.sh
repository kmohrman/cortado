# Run over one test sample
python run_skimmer.py ../../input_samples/sample_jsons/test_samples/for_ci.json -x local -o "skimtest"

# Example of a check of the number of events
python check_nanoevents_example.py
