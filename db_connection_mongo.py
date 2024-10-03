from pymongo import MongoClient
import datetime
import string

def connectDataBase():

    # Creating a database connection object using pymongo

    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")

def createDocument(documents, docId, docText, docTitle, docDate, docCat):

    doc = {
        "_id": docId,
        "docText": docText,
        "docTitle": docTitle,
        "docDate": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "docCat": docCat
    }

    documents.insert_one(doc)
    pushTerms(documents, docId, docText)

def pushTerms(documents, docId, docText):
    docText = removePunctuation(docText)
    docText = docText.lower().split()

    for word in docText:
        #query documents for current word
        term = documents.find_one( {"_id": docId, "terms.term": word} )
        # print(term)

        #if word is already in $terms.term, increment $terms.count
        if term is not None:
            documents.update_one( {"_id": docId, "terms.term": word}, {"$inc": {"terms.$.count": 1} } )
           
        #Otherwise, push new term
        else:
            terms = {"$push": {"terms": { 
                                    "term": word,
                                    "count": 1,
                                    "num_chars": len(word)
                                    } }}
            
            documents.update_one( {"_id": docId}, terms )

def removePunctuation(text):
    text = text.translate(str.maketrans('', '', string.punctuation))
    return text


def updateDocument(documents, docId, docText, docTitle, docDate, docCat):
    emptyTerms = { "$set": {
        "terms": []
        }}

    documents.update_one({"_id": docId}, emptyTerms)

    doc = { "$set": {
        "docText": docText,
        "docTitle": docTitle,
        "docDate": datetime.datetime.strptime(docDate, "%Y-%m-%d"),
        "docDate": docDate,
        "docCat": docCat
        }}

    documents.update_one({"_id": docId}, doc)
    pushTerms(documents, docId, docText)


def deleteDocument(documents, docId):
    documents.delete_one({"_id":docId})

def getIndex(documents):
    # for x in documents.find():
    #     print(x)

    pipe = [
            {
                '$unwind': {
                    'path': '$terms'
                }
            }, {
                '$group': {
                    '_id': '$terms.term', 
                    'termCount': {
                        '$push': {
                            'title': '$docTitle', 
                            'count': '$terms.count'
                        }
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }
            ]
    
    inverted_index = documents.aggregate(pipe)

    output = {}

    for x in inverted_index:
        keys = x.keys()
        
        temp_dict = {}
        for y in x.get('termCount'):
            temp_dict[y.get('title')] = y['count']

        output[x.get('_id')] = temp_dict
        temp_dict = {}


    return output
