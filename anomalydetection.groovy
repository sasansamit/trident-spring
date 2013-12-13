import org.apache.log4j.Logger;


//constants to be used/configured
//time window is 30 seconds

Logger log = Logger.getLogger("anomalydetection.groovy")

def timeWindow = 30*1000 //5*60*1000
def numEntriesToTriggerFlag = 5
def memoryThreshold = 2000

curMemory = input["memory"]
curTs = input["timestamp"]

state = dataStore.getData(input["deviceid"])

def tsList = []

if (state != null)
{
	tsList = state["tsList"]
}
else
{
	state = ["flagstatus": "false"]
}

if (curMemory > memoryThreshold)
{
	tsList.add(curTs)
}

log.info "list size " + tsList.size()
if (tsList.size() == 0)
{	
	log.info "Returning because list is empty";
	return
}
//remove all entries which are older than the time window required
def maxvar = tsList.max()
tsList.removeAll {maxvar - it > timeWindow}

state["tsList"] = tsList;
//if 5 entries remain, we have problem
if ((tsList.size() >= numEntriesToTriggerFlag) && (state["flagstatus"] == "false"))
{
	log.info "${curMemory} | ${tsList} | ${curTs} | " + tsList.min()
	
	log.info "raising flag for device: " + input["deviceid"]
	state["flagstatus"] = "true"
}
else if (tsList.size() < numEntriesToTriggerFlag)
{	
	if (state["flagstatus"] == "true")
	{
		log.info "${curMemory} | ${tsList} | ${curTs} | " + tsList.min()
		
		log.info "taking down the flag for device: " + input["deviceid"]
		state["flagstatus"] = "false"
	}
}

dataStore.storeData(input["deviceid"], state)

