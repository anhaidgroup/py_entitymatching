import magellan as mg
import pandas as pd
import os
from PyQt4 import QtCore
datasets_path = os.sep.join([mg.get_install_path(), 'datasets', 'test_datasets'])


path_a = os.sep.join([datasets_path, 'A.csv'])
path_b = os.sep.join([datasets_path, 'B.csv'])
path_c = os.sep.join([datasets_path, 'C.csv'])

A = mg.read_csv_metadata(path_a)
B = mg.read_csv_metadata(path_b, key='ID')
C = mg.read_csv_metadata(path_c, ltable=A, rtable=B)

D = mg.label_table(C, 'label')

print(D)
# timer = QtCore.QTimer()
# timer.setInterval(2000) # 2 seconds
# mg._viewapp.loadFinished.connect(timer.start)
# timer.timeout.connect(mg._viewapp.quit)
