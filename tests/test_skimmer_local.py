import subprocess
from os.path import exists

def test_skimmer():
    args = [
        "time",
        "python",
        "analysis/template_4l/run_skimmer.py",
        "input_samples/sample_jsons/test_samples/for_ci.json",
        "-x",
        "local",
        "-o",
        "analysis/template_4l/skimtest/",
    ]

    # Run ewkcoffea
    subprocess.run(args)

    assert (exists('analysis/template_4l/skimtest/for_ci/skimmed-part0.root'))
