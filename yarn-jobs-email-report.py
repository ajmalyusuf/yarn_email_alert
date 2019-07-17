#!/usr/bin/env python

############################################################################
# This is a python script to send email/generate html report with YARN
# application and queue details
# To generate html output, run 'python yarn-jobs-email-report.py --print > report.html'
# For any questions or suggestions please contact : ayusuf@hortonworks.com
############################################################################

###################################################################
######  Parameters ################################################
### --------- Hive Configs ----------------------------------------
### If doAs is False, the script will add a column for RequestUser
hiveDoAs = False
hiveServiceUser = 'hive'
showHiveInstanceType = True
###---------- Thresholds ------------------------------------------
### Default thresholds in MINUTES for queues NOT specifically configured below
WARN_THRESHOLD_MINS = 60
CRIT_THRESHOLD_MINS = 120
### Specific Leaf Queue threshlods : QueueName : (Warning, Critical)
### Please note that there should not be a comma after last item
THRESHOLDS_IN_MINS = {
    'default': (60,120),
    'dev': (60,120),
    'prod'   : (240,480)
}

###---------- RM and ATS server ------------------------------------
### if RM HA is not implemented, leave rm_host_2 as '' (blank)
rm1_host = 'rmhost1.mycompany.com'
rm2_host = 'rmhost2.mycompany.com'
ts_host = 'ats.mycompany.com'

rm_port = '8088'
ts_port = '8188'

# Set security to 'KERBEROS' or 'NONE'
# Please ensure to 'kinit' using 'yarn' service principal
security = 'KERBEROS'

###---------- Email Configurations ---------------------------------
### For testing with GMail use 'smtp.gmail.com:587'
smtpServer = 'email.server.mycompany.com:25'

### True/False based on if TLS is supported in the email server. For GMail, this value is True
tlsSupported = False

### True/False based on if authentication is required for the email server
### For testing with GMail authenticationRequired = True and provide your username and password
### If authenticationRequired=False, then username and password are ignored
authenticationRequired = False
username = 'username'
password = 'password'

### sender and recipients are Mandatory. You may add extra recipients as comma separated
sender = 'Yarn Report <do-not-reply@mycompany.com>'
### Please note that there should not be a comma after last item
recipients = [
    'FirstName LastName <user1@mycompany.com>',
    'FirstName LastName <user2@mycompany.com>'
]

###------------- Filters -------------------------------------------
### appState should be one of [NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED]
appStates = 'NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING'
#appStates = 'KILLED,FAILED,FINISHED'    # Only used for testing as there may not be many active jobs

### hourFilter in days. Will filter apps with 'startedTime' within the last 'hourFilter' hours
### For example, a value of 24 means, it will filter apps started in the last 24 hours
### Provide 0 to disable date based filtering
hourFilter = 720
##################################################################

import sys
import urllib2
import json
import time
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Colors
barGreen = '#5AD35B'
green = '#009901'
orange = '#FFA447'
red = '#D9534F'
grey = '#ddd'
white = 'white'
black = 'black'

class Application:
    def __init__(self,id,name,queue,state,startedTime,finishedTime,elapsedTime,requestUser,user,progress,appType,trackingUrl):
        self.id = id
        self.name = name
        self.queue = queue
        self.state = state
        self.startedTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(startedTime)/1000))
        self.finishedTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(finishedTime)/1000))
        self.elapsedTime = int(elapsedTime)
        self.requestUser = requestUser
        self.user = user
        self.progress = '%.1f%s' % (float(progress), '%')
        self.appType = appType
        self.trackingUrl = trackingUrl

    def Convert(self, milliseconds):
        millis = milliseconds%1000
        seconds = int(milliseconds/1000)%60
        minutes = int(milliseconds/(1000*60))%60
        hours = int(milliseconds/(1000*60*60))%24
        return hours, minutes, seconds, millis
    def GetFormattedElapsedTime(self):
        hours, minutes, seconds, millis = self.Convert(self.elapsedTime)
        return FormatTimeInSecs(int(self.elapsedTime/1000))

class HTML:
    tabFormat = ''
    font = 'Calibri, sans-serif'
    iconFont = 'Courier, monospace'
    fontSize = 'font-size: 12px;' #Currently not used

    styleTRBottomBorder = 'style="border-bottom: 1px solid %s;"'
    styleTRTopBottomBorder = 'style="border-top: 1px solid %s; border-bottom: 1px solid %s;"'

    styleIconSpan = 'style="color: %s; background-color: %s; border: 1px solid %s;"'
    styleIconTDTH = 'style="font-family: %s; padding: 0px; text-align: center; white-space: nowrap;"' % (iconFont)
    styleIconStringTDTH = 'style="font-family: %s; padding: 0px; padding-left: 2px; text-align: left;"' % (font)

    styleHTagText = 'style="font-family: %s; padding: 4px;"' % (font)

    styleRegText = 'style="font-family: %s; text-align: left;"' % (font)
    styleHdrCellLeft = 'style="font-family: %s; padding: 4px; border-top: 1px solid %s;border-bottom: 1px solid %s;background-color: %s;text-align: left;"' % (font, grey, grey, grey)
    styleRegCellLeft = 'style="font-family: %s; padding: 4px; border-bottom: 1px solid %s;text-align: left;"' % (font, grey)

    styleRegFullRowLeft = 'style="font-family: %s; padding: 4px; padding-top: 6px; padding-bottom: 6px; border-bottom: 1px solid %s;text-align: left;"' % (font, grey)

    styleHdrCellCenter = 'style="font-family: %s; padding: 4px; border-top: 1px solid %s; border-bottom: 1px solid %s; background-color: %s;text-align: center;"' % (font, grey, grey, grey)
    styleRegCellCenter = 'style="font-family: %s; padding: 4px; border-bottom: 1px solid %s;text-align: center;"' % (font, grey)

    styleHdrCellRight = 'style="font-family: %s; padding: 4px; border-top: 1px solid %s;border-bottom: 1px solid %s;background-color: %s;text-align: right;"' % (font, grey, grey, grey)
    styleRegCellRight = 'style="font-family: %s; padding: 4px; border-bottom: 1px solid %s;text-align: right;"' % (font, grey)

    styleTDWidthLeftTemplate =  'style="width: %s; font-family: %s; text-align: left; border: 0px;cellpadding: 0px;cellspacing: 0px"' % ('%.2f%s', font)
    styleTDWidthRightTemplate = 'style="width: %s; font-family: %s; text-align: right; border: 0px;cellpadding: 0px;cellspacing: 0px"' % ('%.2f%s', font)

    styleTDBarWidthBkCTemplate = 'style="width: %s;font-family: %s; background-color: %s; white-space: nowrap; border: 0px;cellpadding: 0px;cellspacing: 0px"' % ('%.2f%s', font, '%s')


class Threshold:
    @staticmethod
    def GetThresholdMap():
        thresholdList = []
        for queueName in sorted(THRESHOLDS_IN_MINS):
            warnThreshold, critThreshold = THRESHOLDS_IN_MINS[queueName]
            thresholdList.append((queueName, warnThreshold, critThreshold))
        # Populate default threshoolds for non-configured queueNames
        thresholdList.append(('Other queues', WARN_THRESHOLD_MINS, CRIT_THRESHOLD_MINS))
        return thresholdList
            
    @staticmethod
    def CheckViolation(elapsedTimeInMillisecs, queueName):
        if queueName in THRESHOLDS_IN_MINS:
            warnThreshold, critThreshold = THRESHOLDS_IN_MINS[queueName]
        else:
            warnThreshold, critThreshold = WARN_THRESHOLD_MINS, CRIT_THRESHOLD_MINS
            
        if int(elapsedTimeInMillisecs/1000) > critThreshold * 60:
            return red, 'C'
        elif int(elapsedTimeInMillisecs/1000) > warnThreshold * 60:
            return orange, 'W'
        else:
            return green, 'O'

class Queues:
    queues = []

    def PopulateQueues(self, queueInfo, parent = None):
        name = queueInfo['queueName']
        capacity = queueInfo['capacity']
        usedCapacity = queueInfo['usedCapacity']

        if 'type' in queueInfo:
            leafQueue = True
        else:
            leafQueue = False

        if parent == None:
            state = 'Top Most Queue'
            numApplications = 0
            numActiveApplications = 0
            numPendingApplications = 0
            absoluteUsedCapacity = usedCapacity
            parentForNext = '%s' % (name)
        else:
            state = queueInfo['state']
            numApplications = queueInfo['numApplications']
            absoluteUsedCapacity = queueInfo['absoluteUsedCapacity']
            if leafQueue:
                numActiveApplications = queueInfo['numActiveApplications']
                numPendingApplications = queueInfo['numPendingApplications']
            else:
                numActiveApplications = numApplications
                numPendingApplications = 0
            parentForNext = '%s.%s(%.1f%s)' % (parent, name, capacity, '%')

        self.queues.append((parent, name, capacity, leafQueue, usedCapacity, absoluteUsedCapacity, state, numApplications, numActiveApplications, numPendingApplications))

        if 'queues' in queueInfo:
            if 'queue' in queueInfo['queues']:
                for queue in queueInfo['queues']['queue']:
                    self.PopulateQueues(queue, parentForNext)

    def GetQueueList(self, queueInfo):
        self.PopulateQueues(queueInfo)
        return self.queues

def FormatTimeInSecs(timeInSeconds):
    seconds = int(timeInSeconds)%60
    minutes = int(timeInSeconds/(60))%60
    hours = int(timeInSeconds/(60*60))
    dateString = ''
    addedHours = False
    if hours > 0:
        dateString += '%dh ' % hours
        addedHours = True
    if addedHours or minutes > 0:
        dateString += '%02dm ' % minutes
    dateString += '%02ds' % seconds
    return dateString

def FormatTimeInMins(timeInMinutes):
    minutes = int(timeInMinutes)%60
    hours = int(timeInMinutes/60)
    dateString = ''
    addedHours = False
    if hours > 0:
        dateString += '%dh ' % hours
        addedHours = True
    dateString += '%02dm' % minutes
    return dateString


def GetQueueStatus():
    url = 'http://%s:%s/ws/v1/cluster/scheduler'

    active_rm_host, schedulerData = GetURLData(url, 'scheduler')
    # print json.dumps(schedulerData, indent=4, sort_keys=True)

    try:
        queueInfo = schedulerData['schedulerInfo']
        queues = Queues()
        queueInfoList = queues.GetQueueList(queueInfo)
    except Exception as e:
        Log(sys.stderr,'Unable to get Queue information.')
        Log(sys.stderr, 'Exception:')
        Log(sys.stderr, str(e))
        sys.exit(0)
    else:
        return queueInfoList

def GetLegendTable():
    html = '<table style="border-collapse:collapse;width: 100%;">\n'

    html += '<tr>\n'
    html += '<th %s>%s</th>\n' % (HTML.styleHdrCellLeft, 'Queue Name')

    html += '<th %s>\n' % (HTML.styleHdrCellLeft)
    html += '<table>\n'
    html += '<tr>\n'
    html += '<th %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></th>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (white, green, white), 'O')
    html += '<th %s>%s</th>\n' % (HTML.styleIconStringTDTH, 'OK Threshold')
    html += '</tr>\n'
    html += '</table>\n'
    html += '</th>\n'

    html += '<th %s>\n' % (HTML.styleHdrCellLeft)
    html += '<table>\n'
    html += '<tr>\n'
    html += '<th %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></th>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (white, orange, white), 'W')
    html += '<th %s>%s</th>\n' % (HTML.styleIconStringTDTH, 'Warning Threshold')
    html += '</tr>\n'
    html += '</table>\n'
    html += '</th>\n'
    html += '<th %s>\n' % (HTML.styleHdrCellLeft)
    html += '<table>\n'
    html += '<tr>\n'
    html += '<th %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></th>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (white, red, white), 'C')
    html += '<th %s>%s</th>\n' % (HTML.styleIconStringTDTH, 'Critical Threshold')
    html += '</tr>\n'
    html += '</table>\n'
    html += '</th>\n'

    #html += '<th %s><span style="color: %s;">&#9724;&nbsp;</span>%s</th>\n' % (HTML.styleHdrCellLeft, orange, 'Warning Threshold')
    #html += '<th %s><span style="color: %s;">&#9724;&nbsp;</span>%s</th>\n' % (HTML.styleHdrCellLeft, red, 'Critical Threshold')
    html += '</tr>\n'

    thresholdMap = Threshold.GetThresholdMap()
    for queueName, warnTh, critTh in thresholdMap:
        html += '<tr>\n'
        html += '<td %s>%s</td>\n' % (HTML.styleRegCellLeft, queueName)

        html += '<td %s>\n' % (HTML.styleRegCellLeft)
        html += '<table>\n<tr>\n'
        html += '<td %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></td>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (grey, white, grey), 'T')
        html += '<td %s>%s %s</td>\n' % (HTML.styleIconStringTDTH, '<=', FormatTimeInMins(warnTh))
        html += '</tr>\n</table>\n'
        html += '</td>\n'

        html += '<td %s>\n' % (HTML.styleRegCellLeft)
        html += '<table>\n<tr>\n'
        html += '<td %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></td>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (grey, white, grey), 'T')
        html += '<td %s>%s %s and %s %s</td>\n' % (HTML.styleIconStringTDTH, '>', FormatTimeInMins(warnTh), '<=', FormatTimeInMins(critTh))
        html += '</tr>\n</table>\n'
        html += '</td>\n'

        html += '<td %s>\n' % (HTML.styleRegCellLeft)
        html += '<table>\n<tr>\n'
        html += '<td %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></td>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (grey, white, grey), 'T')
        html += '<td %s>%s %s</td>\n' % (HTML.styleIconStringTDTH, '>', FormatTimeInMins(critTh))
        html += '</tr>\n</table>\n'
        html += '</td>\n'

        html += '</tr>\n'
    html += '</table>\n'

    fiterMessage = ''
    if hourFilter > 0:
        fiterMessage = ' submitted in the last %s hour(s)' % hourFilter

    html += '<table style="border-collapse:collapse;width: 100%;">\n'
    html += '<tr>\n'
    html += '<td %s>Configured to check for applications with states: %s%s</td>\n' % (HTML.styleRegFullRowLeft, appStates, fiterMessage)
    html += '</tr>\n'
    html += '<tr>\n'
    #ajmal html += '<td %s>&nbsp;</td>\n' % (HTML.styleRegCellLeft)
    html += '<td>&nbsp;</td>\n'
    html += '</tr>\n'
    html += '</table>\n'

    return html

def GetTimelineURLData(url, baseTag):
    url = url % (ts_host, ts_port)
    # Log(sys.stderr,'TIMELINE_DATA: ' + url) # Ajmal: Comment this
    if security and security.upper() == 'KERBEROS':
        return GetTimelineSecureURLData(url, baseTag)

    try:
        req = urllib2.Request(url)
        response = urllib2.urlopen(req)
        if baseTag != '':
            result = json.loads(response.read())[baseTag]
        else:
            result = json.loads(response.read())
    except Exception as e:
        Log(sys.stderr,'Could not connect to Timeline server: %s:%s' % (ts_host, ts_port))
        Log(sys.stderr, 'Please check TS hostname and port')
        Log(sys.stderr, 'Exception:')
        Log(sys.stderr, str(e))
        sys.exit(0)

    return result    

def GetTimelineSecureURLData(url, baseTag):
    try:
        import pycurl
        import cStringIO
        url = str(url)
        c = pycurl.Curl()
        c.setopt(pycurl.URL, url)
        s = cStringIO.StringIO()
        c.setopt(c.WRITEFUNCTION, s.write)
        c.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_GSSNEGOTIATE)
        c.setopt(pycurl.USERPWD, ':')
        c.perform()
        if c.getinfo(pycurl.HTTP_CODE) != 200:
            errorString = "Error executing the URL: '%s' for the username: '%s'\n" % (url, username)
            sys.stderr.write(errorString)
            sys.exit(0)
        if baseTag != '':
            result = json.loads(s.getvalue())[baseTag]
        else:
            result = json.loads(s.getvalue())
    except Exception as e:
        Log(sys.stderr,'Could not connect to Timeline server: %s:%s' % (ts_host, ts_port))
        Log(sys.stderr, 'Please check TS hostname and port or if kinit is successfully done')
        Log(sys.stderr, 'Exception:')
        Log(sys.stderr, str(e))
        sys.exit(0)
    return result

def write_to_json_file(data, filename):
    if not filename.endswith('.json'):
        filename = filename + '.json'
    f = open(filename,'w') 
    f.write(json.dumps(data, indent=4, sort_keys=True))
    f.close()

def GetURLData(url, baseTag):
    try:
        url1 = url % (rm1_host, rm_port)
        # Log(sys.stderr,'RM_DATA: ' + url1) # Ajmal: Comment this before deployment
        req = urllib2.Request(url1)
        response = urllib2.urlopen(req)
        runningApps = json.loads(response.read())[baseTag]
    except:
        if rm2_host == '':
            Log(sys.stderr,'Could not connect to the RM Hosts: %s:%s' % (rm1_host, rm_port))
            Log(sys.stderr, 'Please check RM hostname(s) and port')
            Log(sys.stderr, 'Exception:')
            Log(sys.stderr, str(e))
            sys.exit(0)
        try:
            url2 = url % (rm2_host, rm_port)
            req = urllib2.Request(url2)
            response = urllib2.urlopen(req)
            runningApps = json.loads(response.read())[baseTag]
        except Exception as e:
            Log(sys.stderr,'Could not connect to either of the RM Hosts: %s:%s or %s:%s' % (rm1_host, rm_port, rm2_host, rm_port))
            Log(sys.stderr, 'Please check RM hostname(s) and port')
            Log(sys.stderr, 'Exception:')
            Log(sys.stderr, str(e))
            sys.exit(0)
        else:
            active_rm_host = rm2_host
    else:
        active_rm_host = rm1_host
    return active_rm_host, runningApps

def GetMailContent():
    Log(sys.stderr, 'Checking applications with states: %s' % appStates)

    if hourFilter > 0:
        fromTimeInEpoc = (int(time.time()) - int(hourFilter * 60 * 60)) * 1000
        url = 'http://%s:%s/ws/v1/cluster/apps?states=' + appStates + '&startedTimeBegin=' + str(fromTimeInEpoc)
    else:
        url = 'http://%s:%s/ws/v1/cluster/apps?states=' + appStates

    active_rm_host, runningApps = GetURLData(url, 'apps')
    if runningApps is None:
        Log(sys.stderr,'There are No apps with states: %s' % appStates)
        sys.exit(0)

    sortedRunningApps = sorted(runningApps['app'], key=lambda x: x['startedTime'], reverse=True)
    #print json.dumps(sortedRunningApps, indent=4, sort_keys=True)
    #sys.exit(0)

    runningList = []
    count = 0
    filterCount = 0
    for app in sortedRunningApps:
        count += 1
        # print int(app['startedTime'])/1000, fromTimeInEpoc, (int(app['startedTime'])/1000 - fromTimeInEpoc)
        
        filterCount += 1
        if app['trackingUI'] == 'UNASSIGNED':
            trackingUrl = ''
        else:
            trackingUrl = app['trackingUrl']
        appId = app['id']
        if app['name'] and app['name'].startswith('HIVE-'):
            appName = 'HIVE'
        elif app['name'] and app['name'].startswith('oozie:'):
            indx = app['name'].find(':', 7)
            if indx > 0:
                appName = app['name'][:indx]
            else:
                appName = 'OOZIE'
        elif len(app['name']) > 30:
            appName = app['name'][:30] + '...'
        else:
            appName = app['name']
        appUser = app['user']

        if appName == 'HIVE' and (hiveDoAs == False or showHiveInstanceType == True):
            requestUser, hiveInstanceType = get_hive_request_user(appId, appUser)
            if hiveInstanceType != '':
                appName = appName + ' - ' + hiveInstanceType
        else:
            requestUser = '&nbsp;-'

        newApp = Application(appId, appName, app['queue'], app['state'], app['startedTime'],\
                app['finishedTime'], app['elapsedTime'], requestUser, appUser,\
                app['progress'], app['applicationType'], trackingUrl)
        runningList.append(newApp)
    
    Log(sys.stderr, 'Filtered %s of %s application(s)' % (filterCount, count))
    return active_rm_host, runningList

def get_hive_request_user(appId, appUser):
    dagIdUrl = 'http://%s:%s/ws/v1/timeline/TEZ_DAG_ID?limit=1000&primaryFilter=applicationId:' + appId
    dagEntities = GetTimelineURLData(dagIdUrl, 'entities')
    
    # write_to_json_file(dagEntities, appId)

    requestUser = '&nbsp;-'
    hiveInstanceType = ''
    nonBlankNonServiceRequestUser = ''
    for dag in dagEntities:
        if 'callerType' in dag['otherinfo'] and dag['otherinfo']['callerType'] == 'HIVE_QUERY_ID':
            if 'callerId' in dag['otherinfo']:
                tsUrl = 'http://%s:%s/ws/v1/timeline/HIVE_QUERY_ID/' + dag['otherinfo']['callerId']
                appData = GetTimelineURLData(tsUrl, '')
                #write_to_json_file(appData, appId + '__' + dag['otherinfo']['callerId'])

                if 'otherinfo' in appData and 'HIVE_INSTANCE_TYPE' in appData['otherinfo']:
                    hiveInstanceType = str(appData['otherinfo']['HIVE_INSTANCE_TYPE'])

                if 'primaryfilters' in appData and 'requestuser' in appData['primaryfilters']:
                    reqUserFromAppData = str(appData['primaryfilters']['requestuser'][0])

                    if requestUser == '&nbsp;-' or reqUserFromAppData != '':
                        requestUser = reqUserFromAppData
                    if reqUserFromAppData != appUser and reqUserFromAppData != '' :
                        nonBlankNonServiceRequestUser = reqUserFromAppData
                        break
    if nonBlankNonServiceRequestUser != '':
        return nonBlankNonServiceRequestUser, hiveInstanceType
    return requestUser, hiveInstanceType
            
def GetFormattedMessageAsRows(runningApps, active_rm_host):
    plainTxt = 'Not implemented'
    html = '<html>\n'
    html += '<head></head>\n'
    html += '<body>\n'

    queueList = GetQueueStatus()

    if runningApps is None:
        html += '<p>There are no applications in RUNNNING state<br></p>'
    else:
        html += '\n\n'
        html += '<h2 %s><b>YARN Applications Report (RM Host: %s)</b></h2>\n\n' % (HTML.styleHTagText, active_rm_host)
        html += '<table style="width: 100%;">\n'
        html += '<tr>\n'
        html += '<td %s>Generated on:  %s</td>\n' % (HTML.styleRegText, time.strftime("%a, %d %b %Y %I:%M %p %Z"))
        html += '</tr>\n'
        html += '</table>\n'

        html += '\n\n'
        html += '<h2 %s><b>Scheduler Queue Utilization:</b></h2>\n\n' % (HTML.styleHTagText)

        #blankPadding = '      '.replace(' ', '&nbsp;')
        blankPadding = ''

        html += '<table style="width: 100%;">\n'
        for queue in queueList:
            parent, name, capacity, leafQueue, usedCapacity, absoluteUsedCapacity, state, numApps, numActiveApps, numPendingApps = queue

            if not leafQueue:
                continue

            if numPendingApps == 0:
                pendingAppsStr = ''
            else:
                pendingAppsStr = 'Pending jobs: <b>%d</b>, ' % (numPendingApps)

            if usedCapacity > 100:
                barColor = orange
                highlightColor = orange
            else:
                barColor = barGreen
                highlightColor = green
            if parent == None:
                topLeftText = 'Queue: <b>%s</b> (%.1f%s)' % (name, capacity, '%')
                topRightText = 'Total cluster capacity used: <b>%.1f%s</b>' % (usedCapacity, '%')
            else:
                topLeftText = 'Queue: %s.<b>%s</b> (%.1f%s)' % (parent, name, capacity, '%')
                topRightText = 'Active jobs: <b>%d</b>, %sCapacity used: <span style="color: %s;"><b>%.1f%s</b></span>' % (numActiveApps, pendingAppsStr, highlightColor, usedCapacity, '%')

            html += '<tr>\n'
            html += '<td %s>\n' % (HTML.styleTDWidthLeftTemplate % (96, '%'))
            html += '<table style="border-collapse:collapse;width: 100%;">\n'
            html += '<tr>\n'
            html += '<td %s>%s</td>\n' % (HTML.styleTDWidthLeftTemplate % (40, '%'), topLeftText)
            html += '<td %s>%s</td>\n' % (HTML.styleTDWidthRightTemplate % (60, '%'), topRightText)
            html += '</tr>\n'
            html += '</table>\n'
            html += '</td>\n'
            html += '<td %s>%s</td>\n' % (HTML.styleTDWidthRightTemplate % (6, '%'), blankPadding)
            html += '</tr>\n'

            html += '<tr>\n'
            html += '<td %s>\n' % (HTML.styleTDWidthLeftTemplate % (96, '%'))
            html += '<table style="border-collapse:collapse;width: 100%;">\n'
            html += '<tr>\n'
            html += '<td %s>&nbsp;</td>\n' % (HTML.styleTDBarWidthBkCTemplate % (absoluteUsedCapacity, '%', barColor))
            html += '<td %s></td>\n' % (HTML.styleTDBarWidthBkCTemplate % (100-absoluteUsedCapacity, '%', grey))
            html += '</tr>\n'
            html += '</table>\n'
            html += '</td>\n'
            html += '<td %s>%.1f%s</td>\n' % (HTML.styleTDWidthRightTemplate % (6, '%'), absoluteUsedCapacity, '%')
            html += '</tr>\n'
        html += '</table>\n'

        html += '\n\n'
        html += '<h2 %s><b>Active Applications:</b></h2>\n\n' % (HTML.styleHTagText)

        html += GetLegendTable()

        html += '<table style="border-collapse:collapse;width: 100%;">\n'
        html += '<tr>\n'
        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellLeft, 'AppID')
        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellLeft, 'AppName')
        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellLeft, 'Queue')
        if hiveDoAs == False:
            html += '<th %s>%s</th>\n' % (HTML.styleHdrCellLeft, 'RequestUser')
        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellLeft, 'User')

        html += '<th %s>\n' % (HTML.styleHdrCellLeft)
        html += '<table>\n'
        html += '<tr>\n'
        html += '<th %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></th>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (white, grey, white), 'S')
        html += '<th %s>%s</th>\n' % (HTML.styleIconStringTDTH, 'State')
        html += '</tr>\n'
        html += '</table>\n'
        html += '</th>\n'

        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellCenter, 'AppType')
        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellRight, 'StartedTime')
        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellRight, 'ElapsedTime')
        html += '<th %s>%s</th>\n' % (HTML.styleHdrCellRight, 'Progress')
        html += '</tr>\n'
        for app in runningApps:
            iconColor, iconLetter = Threshold.CheckViolation(app.elapsedTime, app.queue)
            
            html += '<tr>\n'
            html += '<td %s><a href="%s">%s</a></td>\n' % (HTML.styleRegCellLeft, app.trackingUrl, app.id)
            html += '<td %s>%s</td>\n' % (HTML.styleRegCellLeft, app.name)
            html += '<td %s>%s</td>\n' % (HTML.styleRegCellLeft, app.queue)
            if hiveDoAs == False:
                html += '<td %s>%s</td>\n' % (HTML.styleRegCellLeft, app.requestUser)
            html += '<td %s>%s</td>\n' % (HTML.styleRegCellLeft, app.user)

            html += '<td %s>\n' % (HTML.styleRegCellLeft)
            html += '<table>\n'
            html += '<tr>\n'
            html += '<td %s><span %s>&nbsp;<b>%s</b>&nbsp;</span></td>\n' % (HTML.styleIconTDTH, HTML.styleIconSpan % (white, iconColor, grey), iconLetter)
            html += '<td %s>%s</td>\n' % (HTML.styleIconStringTDTH, app.state)
            html += '</tr>\n'
            html += '</table>\n'
            html += '</td>\n'

            html += '<td %s>%s</td>\n' % (HTML.styleRegCellCenter, app.appType)
            html += '<td %s>%s</td>\n' % (HTML.styleRegCellRight, app.startedTime)
            html += '<td %s>%s</td>\n' % (HTML.styleRegCellRight, app.GetFormattedElapsedTime())
            html += '<td %s>%s</td>\n' % (HTML.styleRegCellRight, app.progress)
            html += '</tr>\n'
        html += '</table>\n'
    html += '</body>\n'
    html += '</html>\n'

    return plainTxt, html

def Log(outstream, message):
    ts = int(time.time())
    logTime = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    outstream.write(logTime + ': ' + message + '\n')
    

####### Program Start #######
#############################

active_rm_host, runningApps = GetMailContent()

textMsg, htmlMsg = GetFormattedMessageAsRows(runningApps, active_rm_host)

msg = MIMEMultipart('alternative')
msg['Subject'] = 'Alert (RM Host: %s): Long Running jobs' % active_rm_host
msg['From'] = sender
msg['To'] = ", ".join(recipients)

plainPart = MIMEText(textMsg, 'plain')
htmlPart = MIMEText(htmlMsg, 'html')

msg.attach(plainPart)
msg.attach(htmlPart)

if len(sys.argv) >= 2 and (sys.argv[1] == '--print' or sys.argv[1] == '-p'):
    Log(sys.stderr, 'This is TEST mode which will print html instead of sending as email...')
    Log(sys.stderr, 'You can redirect the stdout to a file using "> report.html"...')
    print htmlMsg
    sys.exit(1)

try:
    s = smtplib.SMTP(smtpServer)
    s.ehlo()

    if tlsSupported:
        s.starttls()
    if authenticationRequired:
        s.login(username,password)
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()
except Exception as e:
    Log(sys.stderr, 'Error: Unable to send email...!')
    Log(sys.stderr, 'Exception:')
    Log(sys.stderr, str(e))
else:
    Log(sys.stderr, 'Notification email was sent successfully...')

