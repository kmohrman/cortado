import setuptools

setuptools.setup(
    name='cortado',
    version='0.0.0',
    description='Tools for skimming NanoAOD formatted data with the coffea framework.',
    packages=setuptools.find_packages(),
    package_data={
        "cortado" : [
            "params/*",
        ],
    }
)
