import advising

try:
    envDict = {}
    with open('.env') as envFile:
        for line in envFile:
            if line[0] != "#": key, value = line.strip().split('=')
            envDict[key.strip()] = value.strip()
except:
    print('.env file was not found, but is required for this script. Is your Python session running in the correct directory?\n')
    exit()

[print(f'{key}: {envDict[key]}') for key in envDict]

def removeOldDB():
    #workaround to remove temp.db file
    try:
        from os import remove
        remove('../temp.db')
    except:
        pass
removeOldDB()

advisorAPI = advising.AdvisorAPI()
print('Importing advisorList and associating advisors...')
AdvisorList = advising.CSVTableSubclass(envDict['advisor_list'])
@AdvisorList.mapRows
def advisorDecision(row):
    if row[0] != '' and row[1] == '' and row[2] == '':
        advisorAPI.latestProgramName = row[0] #doing this so that I don't have to initialize an unnecesary global var. Instead, just mapping a new attribute to our instance. 
        advisorAPI.newSet(row[0])
    elif row[1] != '':
        advisorAPI.addAdvisors(advisorAPI.latestProgramName,row[1])
    if row[2] != '':
        advisorAPI.addProgram(advisorAPI.latestProgramName,[row[2],row[3],row[4]])

print('Importing CSV\'s...')
importableCSVs = envDict['import_csvs'].split(',')
for filename in importableCSVs: #can't seem to use this w/ lambda and map
    csvObj = advising.CSVTableSubclass(filename)
    filename = filename.split('\\') #this dedupping is not fully featured, I'm just throwing it in to fix something
    filename = filename[-1]
    if filename == "SGRP with date.csv": csvObj.deDup("EMPLID",lambda x: x[-1]) #orders everything by date, then dedups
    SQLTable = advising.migrateData(csvObj,"sql")
    SQLTable.dataPush()

print('Importing and setting advisor count...')
adv_counts = advising.SQLTableSubclass(db_tablename='advisor_assignment_count')
adv_counts.dataPull('SELECT * FROM advisor_assignment_count WHERE STDNT_ENRL_STATUS = \'Enrolled\'')
[advisorAPI.setAdvisorCount(item[0],int(item[1])) for item in adv_counts.Data]
print('Creating new table...')
#"execute"can be called by any available SQLTableSubclass instance; I used one from above to make things slightly faster
adv_counts.execute('''CREATE TABLE all_assignments AS SELECT current_term.EMPLID, current_term.STUDENT_NAME, current_term.ADVISOR_ID, current_term.ADVISOR_NAME, current_term.DESCR2, current_term.SUBPLAN_DESCR, sgrp_with_date.STDGR_STDNT_GROUP, sgrp_with_date.STDGR_STDNT_GROUP_LDESC, current_term.STDNT_ENRL_STATUS, current_term_enrollment.ENRL_ADD_DT, sgrp_with_date.EFFDT, SI_Probation.SRVC_IND_CD, SI_Probation.SRVC_IND_REFRNCE, current_term.K_HOME_CAMPUS, previous_term.REQ_TERM, current_term.ACTION_DT, current_term.PROG_ACTION, current_term.STDNT_CAR_NBR
FROM (((((current_term LEFT JOIN previous_term ON current_term.EMPLID = previous_term.EMPLID) LEFT JOIN sgrp_with_date ON current_term.EMPLID = sgrp_with_date.EMPLID) LEFT JOIN SI_Probation ON current_term.EMPLID = SI_Probation.EMPLID) LEFT JOIN personal_email ON current_term.EMPLID = personal_email.EMPLID) LEFT JOIN current_term_enrollment ON current_term.EMPLID = current_term_enrollment.EMPLID) LEFT JOIN current_term_owtctr ON current_term.EMPLID = current_term_OWTCTR.EMPLID ORDER BY current_term_enrollment.ENRL_ADD_DT DESC;''')

print('Grabbing and setting dictionary for exemptions...')
exemptions = advising.CSVTableSubclass(envDict['exemptions'])
exempt_dict = {int(row[0]): row for row in exemptions.Data} #row[0] is the student ID

print('Generating all_assignments as an ordered, unfiltered list.')
all_assignments = advising.SQLTableSubclass(db_tablename='all_assignments') #instance of class attached to all_assignments table
all_assignments.dataPull(select_statement='SELECT * FROM all_assignments ORDER BY emplid') #set data for the instance to an ordered list
untouched_advisors = envDict['UNTOUCHABLE_ADVISORS'].split(';')
all_assignments_filtered = [row for row in all_assignments.Data if row[8] == 'E' and row[3] not in untouched_advisors] #filter by enrollment ('E') and advisors

print('Using advisor associations to test advisor/program matches...')
#turn this into a function
for row in all_assignments_filtered:
    findCells = lambda line: (line[3],[line[4],line[5],line[6]]) 
    advisorCell,programCells = findCells(row) #uses lambda to grab relevant cells
    suggestCell = advisorAPI.testProgramAdvisor(advisorCell,programCells)
    try: 
        suggestCell=f'Exemption with: {exempt_dict[int(row[0])][3]} Reason: {exempt_dict[int(row[0])][4]}' #fails if studentID is not in exemption dictionary
    except: 
        pass
    row.insert(0,suggestCell)
#now, all_assignments_filtered is a list of lists that represent our data

header_list = all_assignments.Columns
header_list.insert(0,"Advisor Suggestion:")

print('Exporting unfiltered advisor assignment file...')
def BlankFirst(single_row):
    if single_row[0].isnumeric(): single_row.insert(0,'Skipped')
    return single_row
all_assignments.mapRows(BlankFirst,inPlace=True)
all_assignments.export(f'{envDict["unfiltered_file"]}')

print('Exporting filtered advisor assignment file...')
filtered_export = advising.CSVTableSubclass()
filtered_export.Columns = header_list
filtered_export.setData(all_assignments_filtered)
filtered_export.deDup('EMPLID')
filtered_export.export(f'{envDict["filtered_file"]}')

exit()