import requests, json, time, os, sys, MySQLdb
from config import *

# Get key value required to access Product Catalog API from environment variable set by secret shell script and assemble header for request; exit if variable not set
if os.environ.get("MY_API_KEY"):
	MY_API_KEY = str(os.environ.get("MY_API_KEY"))
	apiKey = {"ApiKey": MY_API_KEY}
else:
	print "Environment variable not set - cannot proceed"
	sys.exit(2)

######################################### FUNCTION DEFINITIONS #########################################

# Function that iterates through API resopnse to insert each relevant SKU and associated UPC code to myoffers.io database
def insertSkus(styles, type, brandCode, cursor):

	sqlValues = ""

	for items in styles:						# Iterate through each style in the response

		for colors in items["styleColors"]:		# Iterate through each child style color within a style

			for skus in colors["skus"]:			# Iterate through each child sku within a style color
				
				if type == "legacy":			# If processing 'legacy' business units, work with Store UPC value; else, work with online UPC

					if "storeUPC" in skus:		# If a Store UPC value exists for the SKU, append the details to the VALUES portion of the INSERT statement
						
						sqlValues += "({0},{1},{2}), ".format(skus["businessId"], skus["storeUPC"], brandCode)

				elif type == "singleEntity":

					sqlValues += "({0},{1},{2}), ".format(skus["businessId"], skus["onlineUPC"], brandCode)

	# Build the full INSERT statement with complete set of VALUES to upload (trim last 2 characters of VALUES string to get rid of trailing comma and space)
	sqlStatement = "INSERT INTO upc_KH (SKU,UPC,Brand) VALUES {0} ON DUPLICATE KEY UPDATE UPC = VALUES (UPC);".format(sqlValues[:-2])

	# Try/Catch execution of the MySQL INSERT statement
	try:

		# Commit changes to the MySQL database and commit
		cursor.execute(sqlStatement)
		db.commit()						

	except:

		# Rollback if there is an error
		db.rollback()
		db.close()
		
		print "Database error: ", time.asctime( time.localtime(time.time()) )
		sys.exit(2)

	return

# Function that makes Product Catalog API request until successful response obtained, returns that response for processing
def apiRequest(url, key):

	apiResponse = requests.get(url, headers=key)
	apiResponse.close()
	apiStatusCode = apiResponse.status_code

	# Make sure initial request is successful; if not, re-request until successful response obtained
	while apiStatusCode != 200:
		print url, " - ", apiStatusCode, ": ", apiResponse.elapsed
		apiResponse = requests.get(url, headers=key)
		apiResponse.close()
		apiStatusCode = apiResponse.status_code

	return apiResponse

######################################### END OF FUNCTION DEFINITIONS #########################################

print "Start: ", time.asctime( time.localtime(time.time()) )	#Log script start time to console

# Initialization of dictionary containing Product Catalog paths of all online US business units and associated brand code
# Legacy brands will need to be handled differently than Single Entity brands as SE brands do not have Store UPCs that are distinct from Online UPCs
legacyBizUnits = {"br/us": 2, "gp/us": 1, "on/us": 3}
singleEntityBizUnits = {"at/us": 10, "brfs/us": 6, "gpfs/us": 5}

# Set MySQL connection string from config file containing database information and open conncetion
db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)
dbCursor = db.cursor()

# Process each of the *Legacy* business units
for bizUnit in legacyBizUnits:

	# Product Catalog API url to access ALL products for a business unit, including the SKUs
	initialApiUrl = "https://api.gap.com/commerce/product-catalogs/catalog/{0}?&size=333&includeSkus=true".format(bizUnit)

	# Initial Product Catalog API request - this gets the first batch of products to be processed and determines how many total pages need to be iterated through
	catalogResponse = apiRequest(initialApiUrl, apiKey)
	pages = catalogResponse.json()["page"]["totalPages"]				# Grab total number of pages in Product Catalog API response
	print "Total pages to process for {0}: ".format(bizUnit), pages		# Log total number of pages that need to be processed to the console

	# Process initial page of SKUs for insertion to MySQL db
	insertSkus(catalogResponse.json()["_embedded"]["styles"], "legacy", legacyBizUnits[bizUnit], dbCursor)	
	print "1 page of SKUs uploaded for {0}".format(bizUnit)

	# Grab URL of 'next' pagination link in Product Catalog response to process during first iteration of while loop
	nextLink = catalogResponse.json()["_links"]["next"]["href"]

	x = 1	# Initialize counter for while loop that will ensure the entire Product Catalog is processed

	# Process all remaining pages of Product Catalog response
	while x < pages:

		# Make next request of Product Catalog and check the resulting response for SKU of interest
		catalogResponse = apiRequest(nextLink, apiKey)
		insertSkus(catalogResponse.json()["_embedded"]["styles"], "legacy", legacyBizUnits[bizUnit], dbCursor)

		# Grab URL of 'next' pagination link for subsequent request until the data element is no longer in the response (which will only happen during final iteration of loop)
		if "next" in catalogResponse.json()["_links"]:
			nextLink = catalogResponse.json()["_links"]["next"]["href"]

		# Increment counter & log progress to console
		x += 1
		print x, "pages of SKUs uploaded for {0}".format(bizUnit)

# Process each of the *Single Entity* business units
for bizUnit in singleEntityBizUnits:

	# Product Catalog API url to access ALL products for a business unit, including the SKUs
	initialApiUrl = "https://api.gap.com/commerce/product-catalogs/catalog/{0}?&size=333&includeSkus=true".format(bizUnit)

	# Initial Product Catalog API request - this gets the first batch of products to be processed and determines how many total pages need to be iterated through
	catalogResponse = apiRequest(initialApiUrl, apiKey)
	pages = catalogResponse.json()["page"]["totalPages"]				# Grab total number of pages in Product Catalog API response
	print "Total pages to process for {0}: ".format(bizUnit), pages		# Log total number of pages that need to be processed to the console

	# Process initial page of SKUs for insertion to MySQL db
	insertSkus(catalogResponse.json()["_embedded"]["styles"], "singleEntity", singleEntityBizUnits[bizUnit], dbCursor)	
	print "1 page of SKUs uploaded for {0}".format(bizUnit)

	# Grab URL of 'next' pagination link in Product Catalog response to process during first iteration of while loop
	nextLink = catalogResponse.json()["_links"]["next"]["href"]

	x = 1	# Initialize counter for while loop that will ensure the entire Product Catalog is processed

	# Process all remaining pages of Product Catalog response
	while x < pages:

		# Make next request of Product Catalog and check the resulting response for SKU of interest
		catalogResponse = apiRequest(nextLink, apiKey)
		insertSkus(catalogResponse.json()["_embedded"]["styles"], "singleEntity", singleEntityBizUnits[bizUnit], dbCursor)

		# Grab URL of 'next' pagination link for subsequent request until the data element is no longer in the response (which will only happen during final iteration of loop)
		if "next" in catalogResponse.json()["_links"]:
			nextLink = catalogResponse.json()["_links"]["next"]["href"]

		# Increment counter & log progress to console
		x += 1
		print x, "pages of SKUs uploaded for {0}".format(bizUnit)

# Disconnect from MySQL server
db.close()

print "End: ", time.asctime( time.localtime(time.time()) )	# Log script completion ending time to console