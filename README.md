# support-tooling
HWX Private Support tooling repo

##  Scripts

### yarn-jobs-email-report.py
- Purpose:
  - To send email notification on the Scheduler capacity and Active jobs status
- Features:
  - Graphical display of Scheduler Queue capacity snapshot:
    - Capacity, Used Queue Capacity, Used Absolute Cluster Capacity, No. of active and pending jobs, etc
  - Lists all jobs with app states: NEW,NEW_SAVING,SUBMITTED,ACCEPTED and RUNNING
  - Email alert will be sent, if at least 1 active job is availale with the above states
  - Reports Job details like AppIs, User, Queue, Time information and progress,
  - Ability to configure WARN and CRIT threshold on elapsed time for Long Running jobs to highlight it
  - Supports Resource Manager HA
- Jira:
  - https://hortonworks.jira.com/browse/BUG-81130
- Usage:
  - Configurations are provided as variables inside the python script as the script is expected to be run in cron and to avoid passing args or a file as arg
  - Please update the required variables inside the python file in the 'parameters' section and add it to the crontab

- TIP:
  - To run in a test mode by bypassing sending email and redirecting to a file:
  ```
    python yarn-jobs-email-report.py --print > report.html
  ```

Parameters section in the script

```python
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
```
