[![DOI](https://zenodo.org/badge/126246599.svg)](https://zenodo.org/badge/latestdoi/126246599)

# Python for Earth Observation (PyEO)

PyEO is designed to provide a set of portable, extensible and modular Python scripts for machine learning in earth observation and GIS,
including downloading, preprocessing, creation of baseline composites, classification and validation.

Full documentation is available at https://clcr.github.io/pyeo/build/html/index.html

Example notebooks are available at:
- https://github.com/clcr/pyeo_1/tree/main/notebooks
- https://github.com/clcr/pyeo_training_materials

## Requirements
Python library requirements are categorised by Platform (Operating System - OS). For use in the Cloud Processing platform SEPAL - pyeo is already installed in a virtual environment. <!-- This is in anticipation of pyeo SEPAL-wide venv being created -->
SEPAL is a cloud computing platform for geospatial data which offers remote Linux Instances that are customised for performing geospatial analysis in R or Python. More information can be found here: https://github.com/openforis/sepal <br>. 

Package management is performed by Conda, for instructions on how to install Conda, please refer to: https://docs.conda.io/en/latest/.  
*Note: Conda can be installed as part of Anaconda https://www.anaconda.com/*  
<br>

For installation locally on an OS of your choice, see the sections below.  

To install `pyeo_1`, put the following commands into **Bash** (Linux), **Terminal** (Mac) or the **Anaconda Prompt** (Windows) <br>

### Ubuntu or MacOS
```bash
conda install -c conda-forge git
git clone https://github.com/clcr/pyeo_1.git
cd pyeo_1
conda env create --file environment.yml --name pyeo_env
conda activate pyeo_env
python -m pip install -e .
```
If you do not want to edit `pyeo_1`, replace `python -m pip install -e .` line with

```bash
python -m pip install -vv .
```

### Windows
```bash
conda install -c conda-forge git
git clone https://github.com/clcr/pyeo_1.git
cd pyeo_1
conda env create --file environment_windows.yml --name pyeo_env
conda activate pyeo_env
python -m pip install -e .
```

If you do not want to edit `pyeo_1`, replace `python -m pip install -e .` line with

```bash
python -m pip install -vv .
```
<br>  

#### A Note on `.ini` file encoding on Windows
If the OS that pyeo is running on is Windows, we have noticed that `pyeo_windows.ini` may need to be saved with `ANSI` encoding instead of the usual `UTF-8`. See [this webpage](https://stackoverflow.com/questions/13282189/missingsectionheadererror-file-contains-no-section-headers) for more details.

## Satellite Imagery Providers
From July 2023, Scihub will be deprecated in favour of the Copernicus Data Space Ecosystem (CDSE). In the meantime, if you wish to download from Scihub, you will need a Scihub account: https://scihub.copernicus.eu/

To use the CDSE, you will need a separate account: https://dataspace.copernicus.eu

To process Sentinel-2 L1Cs, you will also need Sen2Cor installed: http://step.esa.int/main/third-party-plugins-2/sen2cor/. This installation process is covered in the PyEO_I_Setup.ipynb notebook, available from the notebooks folder.  
<br>

<!-- ## Installation on SEPAL



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
1. Double click the file `PyEO_1_Setup_on_SEPAL.ipynb` and follow the contained instructions to setup the PyEO conda environment.  -->
<br>

<!-- For Linux users, you can optionally access the `pyeo_1` command line functions, by adding the following to your .bashrc

```bash
export pyeo_1=/path/to/pyeo_1
export PATH=$PATH:$pyeo_1/bin
``` -->
<br>  

## Installation Test Steps

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
<br>  

## How to Run PyEO
PyEO can be run interactively in the Jupyter Notebooks provided in the Tutorials, but the pipeline method can be run via the **Terminal**.  This process is automated and is suited to the advanced python user. <br> 
Both the terminal and notebook methods rely on an a configuration file (e.g. `pyeo_linux.ini`, `pyeo_windows.ini`, `pyeo_sepal.ini`) to make processing decisions.  <br>
The below example references `pyeo_sepal.ini`, but this can be switched for the Linux or Windows equivalent. <br>
<!-- add ini file examples here -->

1. First, move to where PyEO is installed:
```bash
cd pyeo_1
```
2. Now, the pipeline method runs like this. Here we are telling the terminal that we want to invoke `python` to run the script `run_acd_national.py` within the folder `pyeo_1`, then we pass the absolute path to the initialisation file for your OS. The script `run_acd_national.py` requires this path as all the processing parameters are stored in the initialisation file. See below:
```bash
python pyeo_1/run_acd_national.py <insert_your_absolute_path_to>/pyeo_sepal.ini
```

<br>  

## Automated Pipeline Execution
To enable parallel processing of the raster and vector processing pipelines with the `do_parallel = True` option enabled in `pyeo_sepal.ini`, make the following file an executable by issuing this command:
```bash
cd pyeo_1/apps/automation/
chmod u+x automate_launch.sh
```
<br>  

<!-- ## Further Setup Information
A slightly more verbose setup tutorial for `pyeo_1` can be found in the notebooks directory, at PyEO_I_Setup_on_SEPAL.ipynb
<br>  -->

## Tutorials
Once installation of `pyeo_1` is complete, you can follow the tutorial notebooks, which demonstrate the utility of `pyeo_1`.

How to Train Your Classifier: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Model_Training.ipynb

Downloading Sentinel-2 Imagery, Creating a Baseline Composite, Performing Automatic Change Detection: https://github.com/clcr/pyeo_1/blob/main/notebooks/PyEO_I_Master_Tutorials.ipynb
