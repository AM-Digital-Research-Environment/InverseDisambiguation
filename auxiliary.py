##############################################################
# This is for auxiliary functions for making MongoDB test environment,
# getting information from RDSpace, reading json files, etc.
# 2025.02.19.  J. Cheong
##############################################################
import os, json, sys

class auxiliaryDsp2Mgo:
    def extractJson(self,mpath):
        with open(mpath, "r") as f:
            jtxt = json.load(f)
        return jtxt

    def getProjListFromFile(fpath)->list:
        projlist = []
        if os.path.exists(fpath) == False:
            curdir = os.getcwd()
            sys.stderr.write("'%s' does not exist in '%s'.\n" % (fpath, curdir))
        with open(fpath, 'r') as f:
            plines = f.readlines()
            for pline in plines:
                projlist.append(pline.strip().replace('\n',''))
        return projlist
