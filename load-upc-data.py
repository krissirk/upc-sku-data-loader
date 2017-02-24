import requests, json, time, os, sys #, MySQLdbt
from config import *

# Get key value required to access Product Catalog API from environment variable set by secret shell script and assemble header for request; exit if variable not set
if os.environ.get("MY_API_KEY"):
	MY_API_KEY = str(os.environ.get("MY_API_KEY"))
	apiKey = {"ApiKey": MY_API_KEY}
else:
	print "Environment variable not set - cannot proceed"
	sys.exit(2)

# myoffers.io MySQL db credentials 
dbPort = MYSQL_PORT
dbSchema = MYSQL_DATABASE
dbHost = MYSQL_HOST
dbUser = MYSQL_USER
dbPassword = MYSQL_PASSWORD

##################### FUNCTION DEFINITIONS #####################

# Function that iterates through API resopnse to insert each relevant SKU and associated UPC code to myoffers.io database
def insertSkus(styles, type):

	for items in styles:						# Iterate through each style in the response

		for colors in items["styleColors"]:		# Iterate through each child style color within a style

			for skus in colors["skus"]:			# Iterate through each child sku within a style color
				
				if type == "legacy":			# If processing 'legacy' business units, work with Store UPC value; else, work with online UPC

					if "storeUPC" in skus:			
						
						upc = skus["storeUPC"]
						skuId = skus["businessId"]

				elif type == "singleEntity":

					upc = skus["onlineUPC"]
					skuId = skus["businessId"]

				# Insert SKU into database

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

##################### END OF FUNCTION DEFINITIONS ####################

print "Start: ", time.asctime( time.localtime(time.time()) )	#Log script start time to console

# Initialization of tuple containing Product Catalog paths of all online US business units
# Legacy brands will need to be handled differently than single entity brands as single entity brands do not have store UPCs
legacyBizUnits = ("br/us", "gp/us", "on/us")
singleEntityBizUnits = ("at/us", "brfs/us", "gapfs/us")

# Process each of the *legacy* business units
for bizUnit in legacyBizUnits:

	# Product Catalog API url to access ALL products for a business unit, including the SKUs
	initialApiUrl = "https://api.gap.com/commerce/product-catalogs/catalog/{0}?&size=333&includeSkus=true".format(bizUnit)

	# Initial Product Catalog API request - this gets the first batch of products to be processed and determines how many total pages need to be iterated through
	catalogResponse = apiRequest(initialApiUrl, apiKey)
	pages = catalogResponse.json()["page"]["totalPages"]			# Grab total number of pages in Product Catalog API response
	print "Total pages to process for {0}: ".format(bizUnit), pages		# Log total number of pages that need to be processed to the console

	# Check the initial response for SKU of interest
	insertSkus(catalogResponse.json()["_embedded"]["styles"], "legacy")	
	print "1 page of SKUs uploaded"

sys.exit(2)


# Check the initial response for SKU of interest
insertSkus(catalogResponse.json()["_embedded"]["styles"])	
print "1 page uploaded"

# Grab URL of 'next' pagination link in Product Catalog response to process during first iteration of while loop
nextLink = catalogResponse.json()["_links"]["next"]["href"]

x = 1	# Initialize counter for while loop that will ensure the entire Product Catalog is processed

# Process all remaining pages of Product Catalog response
while x < pages:

	# Make next request of Product Catalog and check the resulting response for SKU of interest
	catalogResponse = apiRequest(nextLink, apiKey)
	insertSkus(catalogResponse.json()["_embedded"]["styles"])

	# Grab URL of 'next' pagination link for subsequent request until the data element is no longer in the response (which will only happen during final iteration of loop)
	if "next" in catalogResponse.json()["_links"]:
		nextLink = catalogResponse.json()["_links"]["next"]["href"]

	# Increment counter & log progress to console
	x += 1
	print x, "pages processed"

print "End: ", time.asctime( time.localtime(time.time()) )	# Log script completion ending time to console