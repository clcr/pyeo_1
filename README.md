[![DOI](https://zenodo.org/badge/126246599.svg)](https://zenodo.org/badge/latestdoi/126246599)

# pyeo_1
Python for Earth Observation

This is designed to provide a set of portable, extensible and modular Python scripts for machine learning in earth observation and GIS,
including downloading, preprocessing, creation of base layers, classification and validation.

Full documentation at https://clcr.github.io/pyeo/build/html/index.html

Example notebooks are at https://github.com/clcr/pyeo_training_materials

## Requirements
Package management is performed by Conda: https://docs.conda.io/en/latest/

For downloading, you will need a Scihub account: https://scihub.copernicus.eu/

For processing of Sentinel-2 L1Cs, you will need Sen2Cor installed: http://step.esa.int/main/third-party-plugins-2/sen2cor/, but this is installation process is covered in the PyEO_I_Setup.ipynb notebook: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Setup.ipynb

## Installation
To install `pyeo_1`, put the following commands into **Bash** (Linux), **Terminal** (Mac) or the **Anaconda Prompt** (Windows)

```bash
git clone https://github.com/clcr/pyeo_1.git
cd pyeo_1
conda env create --file environment.yml --name pyeo_1_env
conda activate pyeo_1_env
python -m pip install -e .
```
If you want access to the pyeo_1 command line functions, add the following to your .bashrc (these instructions are only applicable to Bash).

```bash
export pyeo_1=/path/to/pyeo_1
export PATH=$PATH:$pyeo_1/bin
```

If you do not want to edit pyeo_1, replace the pip install line with

```bash
python -m pip install . -vv
```

You can test your installation with by typing the following in Bash/Terminal/Anaconda Prompt:
```
python
import pyeo_1.classification
```

## Further Setup Information
A slightly more verbose setup tutorial for `pyeo_1` can be found at: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Setup.ipynb

## Tutorials
Once installation of `pyeo_1` is complete, you can follow the tutorial notebooks, which demonstrate the utility of `pyeo_1`.

How to Train your Classifier: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Model_Training.ipynb

Downloading Sentinel-2 Imagery, Creating a Baseline Composite, Performing Automatic Change Detection: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Master_Tutorials.ipynb
