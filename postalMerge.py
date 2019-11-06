import subprocess, pymongo, sys, csv

dbName = "uwwPostalCodeMappingDB"
#hostName = "192.168.99.100"
hostName = "localhost"
myclient = pymongo.MongoClient(hostName, 27017)

"""
dbName: the name of the database where you want to insert your collection
collectionName: the name of your collection
hostName: the ip address of the mongo database you want to insert into
path: the path of the file you want to import
"""
def mongoimport(dbName, collectionName, hostName, path):
	removeIDs = []
	myclient = pymongo.MongoClient(hostName, 27017)
	db = myclient[dbName]
	myCollectionName = db[collectionName]
	myCollectionName.drop()
	csv.field_size_limit(sys.maxsize)
	with open(path, encoding='utf-8-sig') as csvfile:
		ll = csv.reader(csvfile, delimiter=',')
		myList = list(ll)
	for x in myList[0]:
		myCollectionName.insert_one({x:''})
	for x in myCollectionName.find({}):
		removeIDs.append(x["_id"])
	header = myList[0]
	del myList[0]
	dictionary = []
	for x in myList:
		dictionary.append(dict(zip(header, x)))
	myCollectionName.insert_many(dictionary)
	for x in removeIDs:
		myCollectionName.delete_one({"_id": x})

"""
fileName: the path/file name you want to export to (include .csv in the file name)
dbName: the name of the database you want to export from
collectionName: the name of the collection you want to export
fieldNames: an array of the names of the fields in the collection you want to export
"""	
def mongoexport(fileName, dbName, collectionName, fieldNames):
	db = myclient[dbName]
	myCollectionName = db[collectionName]
	f = open(fileName, 'w')
	for i in range(len(fieldNames)):
		if(i == len(fieldNames)-1):
			f.write(fieldNames[i] + '\n')
		else:	
			f.write(fieldNames[i] + ', ')
	for x in myCollectionName.find({}):
		for i in range(len(fieldNames)):
			if(i == len(fieldNames)-1):
				f.write(str(x[fieldNames[i]]) + '\n')
			else:
				f.write(str(x[fieldNames[i]]) + ', ')
		#f.write(str(x['_id']) + ', ' + x['postalCodes'] + '\n')
	f.close()
		
	

	
mongoimport(dbName, "usPostalCodes", hostName, "C:\\Users\\josia\\Desktop\\newPostalData\\special_for_spc.csv")
mongoimport(dbName, "canadianPostalCodes", hostName, "C:\\Users\\josia\\Desktop\\newPostalData\\postalByUWCpipelist.csv")
mongoimport(dbName, "uwwLuwMapping", hostName, "C:\\Users\\josia\\Desktop\\newPostalData\\united_way_080219.csv")
	

db = myclient[dbName]

postalArrays = db["postalArrays"]
postalArrays.drop()
usIndMap = db["usIndMap"]
usIndMap.drop()
caIndMap = db["caIndMap"]
caIndMap.drop()
usPostalCodes = db["usPostalCodes"]
canadianPostalCodes = db["canadianPostalCodes"]
uwwLuwMapping = db["uwwLuwMapping"]


print("Updating UWGT OrgID from 601124 to 61500")
continueProcessing = False
retVal =  db["canadianPostalCodes"].update_one(
   { "OrgID": "601124" },
   { "$set": { "OrgID": "61500"}})
	

for x in canadianPostalCodes.find( {"OrgID": "61500"}, { "OrgID": 1,"_id": 0 }):
	continueProcessing = True
	print("UWGT record found: " + x["OrgID"])

if (continueProcessing):
	print("UWGT Update successful, continue processing")
else:
	print("UWGT Update failed, processing halted")


print("Starting aggregation")
result = usPostalCodes.aggregate([{"$group" :{"_id": "$UW Org Number", "postalCodes": { "$push": "$ZIP Code" }}}])
postalArrays = db["postalArrays"]

for x in result:
	postalArrays.insert_one({"_id": -int(x["_id"]), "postalCodes": str(x["postalCodes"])})
	
for x in postalArrays.find({}):
	replacement = x["postalCodes"].replace(",", " |")
	replacement = replacement.lstrip('[')
	replacement = replacement.rstrip(']')
	replacement = replacement.replace("\'", "")
	postalArrays.replace_one({"_id": x["_id"]}, {"postalCodes": replacement})

#if you get a negative ID as not being mapped, it's working properly. Look for it's positive inverse in the CSV files to see if it actually exists
print("Started converting IDs")
for x in postalArrays.find({}):
	idBefore = x["_id"]
	xx = uwwLuwMapping.find_one({"Unitedway_OrganizationNumberKey": str(-int(x["_id"]))})
	if(xx != None):
		postalArrays.insert_one({"_id": xx["Unitedway_Id"], "postalCodes": x["postalCodes"]})
		postalArrays.delete_one({"_id": idBefore})
	else:
		xx = uwwLuwMapping.find_one({"UnitedwaySystem_OrganizationNumber": str(-int(x["_id"]))})
		if(xx != None):
			if(xx["UnitedwaySystem_OrganizationNumber"] != xx["Unitedway_Id"]):
				postalArrays.delete_one({"_id": idBefore})
				postalArrays.insert_one({"_id": xx["Unitedway_Id"], "postalCodes": x["postalCodes"]})
		
		elif (-int(x["_id"]) > 0 and uwwLuwMapping.find_one({"Unitedway_Id": (-int(x["_id"]))}) == None):
			print("Mapping not found: "+ str(x["_id"]))
	
print("Making individual US mapping collection")
docArr = []
usIndMap = db["usIndMap"]
for x in postalArrays.find({}):
	postalArray = x["postalCodes"].split(" | ");
	for y in postalArray:
		docArr.append({"luw_id": x["_id"], "postalCode": y});
usIndMap.insert_many(docArr)

print("Making individual CA mapping collection")
docArr = []
caIndMap = db["caIndMap"]
for x in canadianPostalCodes.find({}):
	postalArray = x["PostalCodes"].split(" | ");
	for y in postalArray:
		docArr.append({"luw_id": x["OrgID"], "postalCode": y});
caIndMap.insert_many(docArr)	
	
print("Merging");
for x in canadianPostalCodes.find({}):
	postalArrays.insert_one({ "_id": x["OrgID"], "postalCodes": x["PostalCodes"]})

print("Exporting")
mongoExport("C:\\Users\\josia\\Desktop\\postalArrays.csv", dbName, 'postalArrays', ['_id', 'postalCodes'])

print("done")

myclient.close()
