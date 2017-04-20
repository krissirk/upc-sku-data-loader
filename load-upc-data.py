import requests, json, time, sys, MySQLdb
from config import *

# Get key value required to access Product Catalog API from config file; build complete header to accompany api request
if API_KEY:
	myHeader = {"ApiKey": API_KEY,
				"User-Agent": "UPC-to-SKU MySQL Loader Python Script",
				"From": CONTACT
				}
else:
	print("API Key not found - cannot proceed")
	sys.exit(2)

######################################### FUNCTION DEFINITIONS #########################################

# Function that iterates through API resopnse to insert each relevant SKU and associated UPC code to myoffers.io database; returns number of rows impacted
def insertSkus(styles, biztype, brandCode, database, cursor):

	sqlValues = ""

	for items in styles:						# Iterate through each style in the response

		for colors in items["styleColors"]:		# Iterate through each child style color within a style

			for skus in colors["skus"]:			# Iterate through each child sku within a style color

				if biztype == "legacy":			# If processing 'legacy' business units, work with Store UPC value; else, work with online UPC

					if "storeUPC" in skus:		# If a Store UPC value exists for the SKU, append the details to the VALUES portion of the INSERT statement

						sqlValues += "({0},{1},{2}), ".format(skus["businessId"], skus["storeUPC"], brandCode)

				elif biztype == "singleEntity":

					sqlValues += "({0},{1},{2}), ".format(skus["businessId"], skus["onlineUPC"], brandCode)

	# If there are SKU-UPC values to insert, proceed with executing the database query and return the resulting row count; else, return '0'
	if sqlValues:

		# Build the full INSERT statement with complete set of VALUES to upload (trim last 2 characters of VALUES string to get rid of trailing comma and space)
		sqlStatement = "INSERT INTO upc (SKU,UPC,Brand) VALUES {0} ON DUPLICATE KEY UPDATE UPC = VALUES (UPC);".format(sqlValues[:-2])

		# Try/Catch execution of the MySQL INSERT statement
		try:

			# Commit changes to the MySQL database and commit
			cursor.execute(sqlStatement)
			database.commit()

		except (MySQLdb.Error, MySQLdb.Warning) as e:

			# Rollback if there is an error
			database.rollback()
			database.close()

			# Log error to the console
			print("Database error: ", time.asctime( time.localtime(time.time()) ), " - ", e)
			print(sqlStatement)
			sys.exit(2)

		return cursor.rowcount

	else:

		return 0

# Function that makes Product Catalog API request until successful response obtained, returns that response for processing
def apiRequest(url):

	apiResponse = requests.get(url, headers=myHeader)
	apiResponse.close()
	apiStatusCode = apiResponse.status_code

	# Make sure initial request is successful; if not, re-request until successful response obtained
	while apiStatusCode != 200:
		print(url, " - ", apiStatusCode, ": ", apiResponse.elapsed)
		apiResponse = requests.get(url, headers=myHeader)
		apiResponse.close()
		apiStatusCode = apiResponse.status_code

	return apiResponse

######################################### END OF FUNCTION DEFINITIONS #########################################

print("Start: ", time.asctime( time.localtime(time.time()) ))	#Log script start time to console

# Initialization of dictionary containing Product Catalog paths & brand codes of all online US business units and associated brand code
# Legacy brands will need to be handled differently than Single Entity brands as SE brands do not have Store UPCs that are distinct from Online UPCs
bizUnits = {'br/us': (2, "legacy"),
			'gp/us': (1, "legacy"),
			'on/us': (3, "legacy"),
			'at/us': (10, "singleEntity"),
			'brfs/us': (6, "singleEntity"),
			'gpfs/us': (5, "singleEntity")
			}

# Set MySQL connection string from config file containing database information and open conncetion
db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)
dbCursor = db.cursor()

# Process each of the business units
for bizUnit in bizUnits:

	# Product Catalog API url to access all approved products within a business unit, including the SKUs
	initialApiUrl = "https://api.gap.com/commerce/product-catalogs/catalog/{0}?&size=200&includeSkus=true&approvalStatus=APPROVED".format(bizUnit)

	# Initial Product Catalog API request - this gets the first batch of products to be processed and determines how many total pages need to be iterated through
	catalogResponse = apiRequest(initialApiUrl)
	pages = catalogResponse.json()["page"]["totalPages"]				# Grab total number of pages in Product Catalog API response
	print("Total pages to process for {0}: ".format(bizUnit), pages)	# Log total number of pages that need to be processed to the console

	# Process initial page of SKUs for insertion to MySQL db
	rowcount = insertSkus(catalogResponse.json()["_embedded"]["styles"], bizUnits[bizUnit][1], bizUnits[bizUnit][0], db, dbCursor)
	print("1 page of SKUs processed for {0} - {1} records updated/inserted".format(bizUnit, rowcount))

	# Grab URL of 'next' pagination link in Product Catalog response if it exists in order to process during first iteration of while loop
	if "next" in catalogResponse.json()["_links"]:
		nextLink = catalogResponse.json()["_links"]["next"]["href"]
		x = 1	# Initialize counter for while loop that will ensure the entire Product Catalog is processed

	else:
		x = pages + 1 # If no 'next' link, initialize counter such that it doesn't go into the while loop

	# Process all remaining pages of Product Catalog response
	while x < pages:

		# Make next request of Product Catalog and process the resulting response
		catalogResponse = apiRequest(nextLink)
		rowcount = insertSkus(catalogResponse.json()["_embedded"]["styles"], bizUnits[bizUnit][1], bizUnits[bizUnit][0], db, dbCursor)

		# Grab URL of 'next' pagination link for subsequent request until the data element is no longer in the response (which will only happen during final iteration of loop)
		if "next" in catalogResponse.json()["_links"]:
			nextLink = catalogResponse.json()["_links"]["next"]["href"]

		# Increment counter & log progress to console
		x += 1
		print(x, "pages of SKUs processed for {0} - {1} records updated/inserted".format(bizUnit, rowcount))

# Disconnect from MySQL server
dbCursor.close()
db.close()

print("End: ", time.asctime( time.localtime(time.time()) ))	# Log script completion ending time to console
