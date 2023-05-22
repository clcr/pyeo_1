import sys
sys.path.append("/home/localadmin1/projects/pyeo_1")
import pyeo_1.core as pyeo_1

def check_pyeo_1_docs(doc_path):
    pyeo_1_func_list = dir(pyeo_1)[20:]
    with open(doc_path, 'r') as index:
        print("Searching for missing functions")
        text = index.read()
        for func in pyeo_1_func_list:
            if text.find(func) == -1:
                print(func)
                
check_pyeo_1_docs(r"/home/localadmin1/projects/pyeo_1/docs/source/index.rst")
