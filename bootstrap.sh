#!/bin/bash -xe

aws s3 cp s3://dm-emr-config/anaconda.sh ~/anaconda.sh 
/bin/bash ~/anaconda.sh -b -p $HOME/conda

echo -e '\nexport PATH=$HOME/conda/bin:$PATH' >> $HOME/.bashrc && source $HOME/.bashrc

pip install msgpack
pip install fuzzywuzzy
pip install  python-levenshtein 

export PYSPARK_PYTHON=/home/hadoop/conda/bin/ipython
export PYSPARK_DRIVER_PYTHON=/home/hadoop/conda/bin/ipython