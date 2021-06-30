import sys,time,datetime,pytz
import requests,json
from requests import Request, Session
from collections import OrderedDict
from apscheduler.schedulers.background import BackgroundScheduler

sleepInterval = int(20)
statusCheckAttempts = int(10)
Login = '<username>'
Password = '<password>'

urlCreateSchedule = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/Schedules'
urlCreateReports = 'https://selectapi.datascope.refinitiv.com/ServiceLayer/Extractions/ReportTemplates'
urlCreateList = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/InstrumentLists'

global done
urlFile = ''
ExtractedFileId = 0

def getAuthToken(header):
    global myToken, URLpartialPath
    urlGetToken = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Authentication/RequestToken'
    
    loginData = json.dumps({'Credentials':{'Password':Password,'Username':Login}})
    resp = requests.post(urlGetToken, loginData, headers=header)
    if resp.status_code != 200:
        print('ERROR, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Get Token failed with ' + str(resp.status_code))
        sys.exit(-1)
    else:
         j = resp.json()
         myToken = j["value"]
         return j["value"] 

def createInstrumentList():
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}

    instrumentListData = json.dumps({"@odata.type": "#DataScope.Select.Api.Extractions.SubjectLists.InstrumentList",
							"Name": "myInstrumentList"})
    resp = requests.post(urlCreateList, instrumentListData, headers=header)
    if resp.status_code != 201:
        print ('ERROR, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Create InstrumentList failed with ' + str(resp.status_code) + str(resp.text))
        sys.exit(-1)
    else:
        k = resp.json();
        print('INFO, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Create Instrument List for ' + k["Name"] + ' sucessfully where ListId = ' + k["ListId"])
        return k["ListId"]   

def appendInstrument(listId, iList):
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}

    urlAppendList = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/InstrumentLists(\'' + listId + '\')/DataScope.Select.Api.Extractions.InstrumentListAppendIdentifiers'
    instrumentList = []
    for instrument in iList:
        instrumentList.append(OrderedDict([("Identifier",instrument),("IdentifierType","Ric")]))
	
    instrumentListData = OrderedDict([("Identifiers", instrumentList),("KeepDuplicates", False)])
    resp = requests.post(urlAppendList, data=json.dumps(instrumentListData), headers=header)
    if resp.status_code != 200:
        print ('ERROR, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Append Instruments failed with ' + str(resp.status_code) + ':' + str(resp.text))
        k = resp.json()
        sys.exit(-1)
    else:
        k = resp.json();
        print('INFO, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Append Instruments successfully' + ' and Appended Instrument Count = ' + str(k["AppendResult"]["AppendedInstrumentCount"]))
          
def createReportTemplate():
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}

    reportData = OrderedDict([("@odata.type","#DataScope.Select.Api.Extractions.ReportTemplates.EndOfDayPricingReportTemplate"),
                              ("ShowColumnHeaders",True),
                              ("Name", "myEodTemplateName"),
                              ("Headers", []),
                              ("Trailers", []),
                              ("ContentFields", [
                                  OrderedDict([("FieldName","Bid Price")]),
                                  OrderedDict([("FieldName","Ask Price")]),
                                  OrderedDict([("FieldName","Trade Date")])
                                  ]),
                              ("Condition",None)])

    resp = requests.post(urlCreateReports, data=json.dumps(reportData), headers=header)
    if resp.status_code != 201:
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Create ReportTemplate failed with ' + str(resp.status_code))
        print('   resp = ', resp.json())
        sys.exit(-1)
    else:
        result = resp.json()
        reportTemplateId = result["ReportTemplateId"]
        print('INFO, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Created Report Templates Id: ' + str(reportTemplateId))  
        return  reportTemplateId

def createScheduleExtration(listIdToUse,reportId):
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}
    
    now = datetime.datetime.utcnow()
    today = now.strftime('%Y-%m-%d')

    schData = OrderedDict([('ListId',listIdToUse),
                           ('Name',"dataAvailabilitySchedule"),
                           ('ReportTemplateId',reportId),
                           ('TimeZone','Coordinated Universal Time'),
                           ('Recurrence',OrderedDict([
                               ('@odata.type','#DataScope.Select.Api.Extractions.Schedules.SingleRecurrence'),
                               ('ExtractionDateTime', today),
                               ('IsImmediate',False)])),
                           ('Trigger',OrderedDict([('@odata.type','#DataScope.Select.Api.Extractions.Schedules.DataAvailabilityTrigger'),('LimitReportToTodaysData',True)]))])
    createDataAvailabilitySchedData = json.dumps(schData, sort_keys=False)
    currDT = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S")
    
    resp = requests.post(urlCreateSchedule, data=createDataAvailabilitySchedData, headers=header)
    if resp.status_code != 200 and resp.status_code != 201:
        print('ERROR,' + currDT + ',Create Schedule Immediate failed with ' + str(resp.status_code))
        print('ERROR,' + currDT + ',' + str(resp.json()))
        sys.exit(-1)
    else:
        m = resp.json()
        print('INFO, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Created Schedule Id: ' + m["ScheduleId"])
        return m["ScheduleId"]

def getInstrumentTriggerDetail(scheduleId):
    global overallArrivalTime
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}
    urlGetInstrumentTriggerDetail = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/ScheduleGetInstrumentTriggerDetails'
    
    urlForAnId = urlGetInstrumentTriggerDetail + '(Id=\'' + scheduleId + '\')'
    overallArrivalTime = None
    resp = requests.get(urlForAnId, headers=header)
    if resp.status_code != 200 and resp.status_code != 201:
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',Create Schedule Immediate failed with ' + str(resp.status_code))
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',' + str(resp.json()))
        sys.exit(-1)
    else:
        m = resp.json()['value']
        print(json.dumps(m, indent=4, sort_keys=True))
        for itemInfo in m:
            if itemInfo['Status'] != 'Arrived':
                #"AverageArrivalUtc": "2020-02-18T06:20:59.000Z"
                arrivalTime = datetime.datetime.strptime(itemInfo['AverageArrivalUtc'] , '%Y-%m-%dT%H:%M:%S.000Z')
                if overallArrivalTime is None:
                    overallArrivalTime = arrivalTime
                elif arrivalTime > overallArrivalTime:
                    overallArrivalTime = arrivalTime

    print ("Overall AverageArrivalUtc for all instruments is " + str(overallArrivalTime) + "(UTC)") 
    return overallArrivalTime

def pollForExtraction(scheduleId):
    global done
    print("Start Polling for Extraction...")
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}
    
    urlGetExtractedFile = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/ExtractedFiles'
    urlGetExtractedIds = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/ReportExtractions'

    URLPollSchedule = urlCreateSchedule+'(\''+ scheduleId + '\')/LastExtraction'
   
    counter=0
    urlFile = ''
    extractionId = 0
    while  done != True and counter < int(statusCheckAttempts): 
        time.sleep(sleepInterval)
        print('INFO, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', (' + str(counter+1) + ') will wait ' + str(sleepInterval) + ' seconds after the data is available ...')
        resp = requests.get(URLPollSchedule, headers=header) 
        if resp.status_code != 200:
            print('WARN, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', GET Schedule status returned ' + str(resp.status_code))
        else:            
            o = resp.json()
            stat = o["Status"]
            if stat == 'Completed':
                # Get list of extracted files
                repExtractionId = o["ReportExtractionId"]
                urlGetExtractedFilesIds = urlGetExtractedIds + '(\'' + repExtractionId + '\')/Files'
                print('INFO, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', Get extracted files reportExtractId url = ' + urlGetExtractedFilesIds)
                resp = requests.get(urlGetExtractedFilesIds, headers=header)
                if resp.status_code!=200:
                    print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',GET Extaction File Id failed with ' + str(resp.status_code))
                    print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',' + resp.json())
                    sys.exit(-1)
                else:
                    p = resp.json()
                    numExtracts = len(p["value"])
                    for idx2 in range(0, numExtracts):
                        ExtractedFileId = p["value"][idx2]["ExtractedFileId"]
                        lnk = p["value"][idx2]["ExtractedFileName"]
                        FileType = p["value"][idx2]["FileType"]
                        if FileType == 'Full':
                            # Only fetch contents of file that has FileType = Full
                            urlFile = urlGetExtractedFile + '(\'' +  ExtractedFileId + '\')/$value'
                            resp = requests.get(urlFile, headers=header)
                            if resp.status_code!=200:
                                print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',GET Extracted file failed with ' + str(resp.status_code))
                                print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',' + resp.json())
                                sys.exit(-1)
                            else:
                                # Dump the contents to a flat CSV file
                                extractFileName = 'extracted_EODPricing_' + datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M%S") + '.csv'
                                fileContent = resp.content # fetches actual content of file from server
                                lenFileContent = len(fileContent)
                                
                                if lenFileContent > 0:
                                    f = open(extractFileName, 'wb')
                                    f.write(fileContent)
                                    f.close()
                                    print('INFO, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', wrote ' + str(len(fileContent)) + ' bytes to file ' + extractFileName)
                                else:
                                    print('WARN, ' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ', no content available for ' + lnk)
                                done = True
    if done == False:
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',Scheduled job did not Complete!')
        sys.exit(-1)

def deleteSchedule(id):
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}
    urlWithId = urlCreateSchedule + '(\'' + id + '\')'
    resp = requests.delete(urlWithId, headers=header)
    if resp.status_code != 204:
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',Delete Schedule failed with ' + str(resp.status_code))
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',' + str(resp.json()))
        sys.exit(-1)   
        
def deleteReportTemplate(id):
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}
    urlWithId = urlCreateReports + '(\'' + id + '\')'
    resp = requests.delete(urlWithId, headers=header)
    if resp.status_code != 204:
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',Delete Report Template failed with ' + str(resp.status_code))
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',' + str(resp.json()))
        sys.exit(-1)   
    
def deleteInstrumentList(id):
    header = {'Content-Type': 'application/json; odata.metadata=minimal', 'Authorization': myToken}
    urlWithId = urlCreateList + '(\'' + id + '\')'
    resp = requests.delete(urlWithId, headers=header)
    if resp.status_code != 204:
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',Delete Report Template failed with ' + str(resp.status_code))
        print('ERROR,' + datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d-%H:%M:%S") + ',' + str(resp.json()))
        sys.exit(-1)   
    
def cleanUp():
    print("Clean up ...")
    deleteSchedule(scheduleId)
    deleteReportTemplate(reportId)
    deleteInstrumentList(listId)
	                       
if __name__ == "__main__":
    global myToken, listId, reportId, scheduleId,done

    # Necessary HTTP header information for most "requests" method we call
    headers = {'Content-Type': 'application/json; odata.metadata=minimal'}

    # STEP 1 get session token
    authToken = getAuthToken(headers) 
    myToken = 'Token '+authToken
    print('INFO, Successfully created token')
    
    # STEP 2 create Instrument List
    listId = createInstrumentList()
    appendInstrument(listId,["BBL.BK","6201.T","0001.HK"])

    # STEP 3 create Eod Report Template
    reportId = createReportTemplate()
    
    # STEP 4 create Data Availability Schedule
    scheduleId = createScheduleExtration(listId, reportId)

    # STEP 5 determine Overall Average Arrival Time in UTC
    overallAverageArrivalUtc = getInstrumentTriggerDetail(scheduleId)

    done = False
    # STEP 6 schedule a background job to attempt data extraction at the arrival time.
    if overallAverageArrivalUtc is None:
        # All trigger's status is Arrived
        pollForExtraction(scheduleId)
        cleanUp()
    else:
        # Some triggers are not arrived
        scheduler = BackgroundScheduler()
        scheduler.timezone= pytz.utc
        scheduler.add_job(pollForExtraction, 'date', run_date=overallAverageArrivalUtc, args=[scheduleId])
        scheduler.start()

        try:
            # This is here to simulate application activity (which keeps the main thread alive).
            while (done==False):
                time.sleep(5)
            cleanUp()
        except (KeyboardInterrupt, SystemExit):
            # Not strictly necessary if daemonic mode is enabled but should be done if possible
            scheduler.shutdown()
            cleanUp()
