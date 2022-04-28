# Generelle Imports

import pandas as pd
import numpy as np
import shopify
import time
import requests
import json
import os
import shutil
import datetime as dt
import paramiko
import sys
import csv
import subprocess

# Pandas settings
pd.set_option('display.max_columns', 70)
pd.set_option('display.max_rows',50)

# Mehrere Outputs pro Zelle
# Standardmässig zeigt Jupyter nur das letzte Restultat

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

# Custom Functions

from f02_startup.i90_custom_functions import *

