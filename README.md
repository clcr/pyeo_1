[![DOI](https://zenodo.org/badge/126246599.svg)](https://zenodo.org/badge/latestdoi/126246599)

# pyeo_1
Python for Earth Observation.

This is designed to provide a set of portable, extensible and modular Python scripts for machine learning in earth observation and GIS,
including downloading, preprocessing, creation of base layers, classification and validation.

Full documentation at https://clcr.github.io/pyeo/build/html/index.html

Example notebooks are at https://github.com/clcr/pyeo_training_materials

## Requirements
Package management is performed by Conda, for instructions on how to install, refer to: https://docs.conda.io/en/latest/.  
*Note: Conda can be installed as part of Anaconda https://www.anaconda.com/*

For downloading, you will need a Scihub account: https://scihub.copernicus.eu/

For processing of Sentinel-2 L1Cs, you will need Sen2Cor installed: http://step.esa.int/main/third-party-plugins-2/sen2cor/, but this is installation process is covered in the PyEO_I_Setup.ipynb notebook, available from the notebooks folder.

## Installation on SEPAL

SEPAL is a cloud computing platform for geospatial data which offers remote Linux Instances that are customised for performing geospatial analysis in R or Python. More information can be found here: https://github.com/openforis/sepal

If you want to use `pyeo_1` on SEPAL, you can follow these customised instructions below:

1. Register for a SEPAL account at https://docs.sepal.io/en/latest/setup/register.html
1. Request processing credits from the SEPAL Team by providing your use case: https://docs.sepal.io/en/latest/setup/register.html#request-additional-sepal-resources
1. Once approved, from the process screen: https://sepal.io/process, follow the steps below.

Press the terminal `>_` tab to open a Linux terminal

Create a pyeo_home directory in your file system:
```bash
mkdir pyeo_home
```

Move into the pyeo_home directory with: 
```bash
cd pyeo_home
```
Check that `git` is installed on your machine by entering in your terminal:
```bash
git -h
```

1. Because SEPAL already provides git, you can skip the git installation step.
    1. If not, install git by following the install instructions on https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
1. Once git is installed, clone a copy of `pyeo_1` into your pyeo_home directory:
```bash
git clone https://github.com/clcr/pyeo_1.git
```
1. Press the spanner shaped tab and click to open JupyterLab
1. When JupyterLab is running navigate to your pyeo_home directory using the panel on the left hand side and then open the 'notebooks' subdirectory
1. Double click the file `PyEO_1_Setup_on_SEPAL.ipynb` and follow the contained instructions to setup the PyEO conda environment.

## Installation on Other Platforms
To install `pyeo_1`, put the following commands into **Bash** (Linux), **Terminal** (Mac) or the **Anaconda Prompt** (Windows)

```bash
git clone https://github.com/clcr/pyeo_1.git
cd pyeo_1
conda env create --file environment.yml --name pyeo_env
conda activate pyeo_env
python -m pip install -e .
```
For Linux users, you can optionally access the `pyeo_1` command line functions, by adding the following to your .bashrc

```bash
export pyeo_1=/path/to/pyeo_1
export PATH=$PATH:$pyeo_1/bin
```



# Installation Test Steps

If you do not want to edit `pyeo_1`, replace the pip install line with

```bash
python -m pip install . -vv
```

You can test your installation with by typing the following in Bash/Terminal/Anaconda Prompt:
```bash
python
```
```python
import pyeo_1.classification
```

or, by running the same import command above, after having started a jupyter notebook via:

```bash
jupyter notebook
```

*Please note, if you are using SEPAL, jupyter notebooks have to be started via a GUI method instead of from Bash, see*: https://user-images.githubusercontent.com/149204/132491851-5ac0303f-1064-4e12-9627-f34e3f78d880.png 

# Further Setup Information
A slightly more verbose setup tutorial for `pyeo_1` can be found in the notebooks directory, at PyEO_I_Setup_on_SEPAL.ipynb

# Tutorials
Once installation of `pyeo_1` is complete, you can follow the tutorial notebooks, which demonstrate the utility of `pyeo_1`.

How to Train Your Classifier: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Model_Training.ipynb

Downloading Sentinel-2 Imagery, Creating a Baseline Composite, Performing Automatic Change Detection: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Master_Tutorials.ipynb
