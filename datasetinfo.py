#!/usr/bin/env python3
import sys
import os
from dbs.apis.dbsClient import DbsApi
from optparse import OptionParser
from dataclasses import dataclass, field
import os

@dataclass
class DatasetInfo:
    datasetname:list
    verbose:bool
    instance:str = "phys03"
    dataset_fileinfo:dict[str] = field( default_factory=dict)
    def getFileListFromDBS3(self):
        dbs = DbsApi(f'https://cmsweb.cern.ch/dbs/prod/{self.instance}/DBSReader')
        for dataset in self.datasetname:
            dataset=dataset.strip()
            if(self.verbose): 
                print(dataset)
            self.dataset_fileinfo[dataset] = dbs.listFiles(dataset = dataset, detail=1)
        return(self.dataset_fileinfo)
    def getFileList(self):
        return(self.dataset_fileinfo)
    def printFiles(self,dataset=None):
        if dataset is None:
            print(self.dataset_fileinfo)
        else :
            if (dataset in self.dataset_fileinfo):
                print(self.dataset_fileinfo[dataset])
            else:
                print(f"Error! {dataset} is not existed.")
    def getFileListWithFormat(self,want_dataset=None):
        fileinfo_format =[]
        for dataset in self.dataset_fileinfo.keys():
            if ( want_dataset is not None and dataset != want_dataset): continue
            for fileinfo in self.dataset_fileinfo[dataset]:
                fileinfo_format.append([fileinfo["logical_file_name"],fileinfo["file_size"],fileinfo["adler32"]])
        return(fileinfo_format)

if __name__ == "__main__":
    usage=f"Usage: {sys.argv[0]} [options] [dataset1] [dataset2] ..."
    parser = OptionParser(usage)
    parser.add_option("-f", "--datasetfile", dest="dataset", help="Dataset list file. If this option is set, arguments is ignored.")
    parser.add_option("-i", "--instance", dest="instance",default="phys03", help="Instance Name [global or phys03(default)]")
    parser.add_option("-v", "--verbose", dest="verbose",action="store_true", help="Verbose mode")
    (options, args) = parser.parse_args()
    if ( options.dataset is not None):
        datasetname = open(options.dataset).readlines()
    else:
        datasetname = args
    ds = DatasetInfo(datasetname=datasetname, verbose=options.verbose, instance=options.instance)
    ds.getFileListFromDBS3()
    filesinfo = ds.getFileListWithFormat()
    for fileinfo in filesinfo:
        lfn, size, checksum = fileinfo
        print(f"LFN : {lfn} / Size: {size} / Adler32: {checksum}\n")

