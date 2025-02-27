##############################################################
# This is to update data in MongoDB with the corrdsponding uris and dois from RDspace
# Bearertoken and working person's name for MongoDB should be stored in ./dictionaries/mongodb_auth.json
# The metadata 'dc.identifier.uri' containing "doi.org" will be copied into the metadata 'url'.
# The metadata 'dc.identifier.uri' containing "handle/rdspace"
# will be copied into the metadata 'identifier' with identifier_type = "Digital object identifier"
# 2024.11.19.  J. Cheong
##############################################################

from pymongo import MongoClient
import auxiliary


class dataFromMgo:
    def __init__(self):
        self.client = MongoClient(auxiliary.auxiliaryDsp2Mgo().extractJson(mpath=r"./dictionaries/mongodb_auth.json")['MongodbAuth'])
        mongoDBAuth = auxiliary.auxiliaryDsp2Mgo().extractJson(r"dictionaries\mongodb_auth.json")
        authLink = mongoDBAuth['MongodbAuth']
        client = MongoClient(authLink)
        self.ubtdb = client.projects_metadata_ubt
        self.ujkzdb = client.projects_metadata_ujkz
        self.unilagdb = client.projects_metadata_unilag
        self.clist = self.ubtdb.list_collection_names()
        self.docDict = dict()
        self.subSet = set()
        self.tagSet = set()
        self.subtagSet = set()
        ## 'word': [projCount, {project ID list}]
        self.subDict = dict()
        self.tagDict = dict()
        self.subtagDict = dict()
        ## mgoDocKeysAll may not be needed.. Later we can delete this perhaps.
        self.mgoDocKeysAll = ['_id', 'dre_id', 'bitstream', 'security', 'collection', 'sponsor', 'project', 'citation', 'url', 'titleInfo',
                             'dateInfo', 'name', 'note', 'subject', 'relatedItems', 'identifier', 'location', 'accessCondition',
                             'typeOfResource', 'genre', 'language', 'physicalDescription', 'abstract', 'tableOfContents', 'targetAudience', 'tags', 'updatedBy']
        # _id, dre_id, bitstream, citation, url, relatedItems,identifier,updatedBy: not included. Always different or not relavant for the semantics
        # security, accessCondition: we need to discuss if they add values in semantics.
        self.mgoDocKeysSel = ['security', 'accessCondition', 'collection', 'sponsor', 'project', 'titleInfo', 'dateInfo', 'name', 'note', 'subject', 'location',
                              'typeOfResource', 'genre', 'language', 'physicalDescription', 'abstract', 'tableOfContents', 'targetAudience', 'tags']
        # Keys with simple data type.
        self.mgoDocKeysSpl = ['note', 'typeOfResource', 'abstract', 'tableOfContents', 'security']

    # Get all docus of a collection in a dictionary of dictionaries.
    # docDict['projID'] = dict of the aggregates in json format.
    ### do it for other projects ujkzdb and uniagdb.
    def getAllDocusAllCollection(self):
        print("collections:", len(self.ubtdb.list_collection_names()), self.ubtdb.list_collection_names())
        for i in range(0, len(self.clist)):
            self.docDict[self.clist[i]] = list(self.ubtdb[self.clist[i]].find({}))
            #print("collection:",self.clist[i], ";", self.docDict.keys(), len(self.docDict[self.clist[i]]))

    def getSubTagSet(self):
        for i in range(0, len(self.clist)):
            subList = self.ubtdb[self.clist[i]].distinct('subject.origLabel')
            for sw in subList:
                self.subSet.add(sw)
                self.subtagSet.add(sw)
            tagList = self.ubtdb[self.clist[i]].distinct('tags')
            for tw in tagList:
                self.tagSet.add(tw)
                self.subtagSet.add(tw)
            #print(self.clist[i], "num. of distinct subject words:", len(subList))
            #print(self.clist[i], "num. of distinct tag words:", len(tagList))
        print("In projects_metadata_ubt: The total num. of distinct subject words:", len(self.subSet))
        print("In projects_metadata_ubt: The total num. of distinct tag words:", len(self.tagSet))
        print("In projects_metadata_ubt: The total num. of distinct subject + tag words:", len(self.subtagSet))

    ### With proper queries on arrays for efficiency.
    def findDiffDoc4EachSubTag(self):
        self.getSubTagSet()
        qsublist = ['Education', 'Learning', 'Young people', 'Nigeria', 'Ethics', 'Africa', 'Interviews']
        qtaglist = ['Lockdown', 'Islam', 'Twi', 'Abortion', 'African Studies', 'Lagos', 'Nigeria', 'Challenges', 'Gender']
        squery = {'subject.origLabel':{'$eq': qsublist[0]}}
        tquery = {'tags':qtaglist[0]}
        sresult = list()
        tresult = list()
        for i in range(0,len(self.clist)):
            print(self.clist[i], "="*15)
            tcolDocu = self.ubtdb[self.clist[i]].find(tquery)
            #print("type of colDocu:", type(colDocu))
            for cdoc in tcolDocu:
                tresult.append(cdoc)
                #print(cdoc)
            scolDocu = self.ubtdb[self.clist[i]].find(squery)
            for sdoc in scolDocu:
                sresult.append(sdoc)
        print("Result for Subject:", qsublist[0], " ===========================")
        shareDocSList = []
        for rdoc in sresult:
            sset = set()
            sset.add(rdoc['project']['id'])
            sset.add(rdoc['dre_id'])
            for j in range(0,len(rdoc['subject'])):
                sset.add(rdoc['subject'][j]['origLabel'])
            shareDocSList.append(sset)
            print(sset)
        print("shareDocSList:", shareDocSList)
        self.printSetDiff(shareDocSList)

        print("Result for Tags:", qtaglist[0], " ===========================")
        shareDocTList = []
        for rdoc in tresult:
            #print(rdoc)
            shareDocTList.append({rdoc['project']['id'], rdoc['dre_id']}.union(set(rdoc['tags'])))
            print(rdoc['project']['id'], rdoc['bitstream'], rdoc['tags'])
        print("   ======================")
        self.printSetDiff(shareDocTList)

    # Print
    @staticmethod
    def printSetDiff(ListOfSets):
        print("printing Set Difference:", len(ListOfSets))
        for k in range(0, len(ListOfSets)):
            for j in range(k+1, len(ListOfSets)):
                print(f"{k}-{j}:",ListOfSets[k] - ListOfSets[j])
                print(f"{j}-{k}:", ListOfSets[j] - ListOfSets[k])





