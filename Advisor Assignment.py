import advising

try:
    envDict = {}
    with open('.env') as envFile:
        for line in envFile:
            if line[0] != "#": key, value = line.strip().split('=')
            envDict[key.strip()] = value.strip()
except:
    print('.env file was not found, but is required for this script. Is your Python session running in the correct directory?\n')

[print(f'{key}: {envDict[key]}') for key in envDict]

advisorAPI = advising.AdvisorAPI()
def advisorDecision(row):
    if row[0] != '' and row[1] == '' and row[2] == '':
        advisorAPI.latestProgramName = row[0] #doing this so that I don't have to initialize an unnecesary global var. Instead, just mapping a new attribute to our instance. 
        advisorAPI.newSet(row[0])
    elif row[1] != '':
        advisorAPI.addAdvisors(advisorAPI.latestProgramName,row[1])
    if row[2] != '':
        advisorAPI.addProgram(advisorAPI.latestProgramName,[row[2],row[3],row[4]])

AdvisorList = advising.CSVObject(envDict['advisor_list'])
AdvisorList.mapRows(advisorDecision)

advisorSQLDB = advising.SQLAPI()

importableCSVs = envDict['import_csvs'].split(',')
for item in importableCSVs: #can't seem to use this w/ lambda and map
    csvObj = advising.CSVObject(item)
    advisorSQLDB.addRows(csvObj.name,csvObj.Data,csvObj.Columns)

advisorSQLDB.arbitraryExecute('''CREATE TABLE all_assignments AS SELECT current_term.EMPLID, current_term.STUDENT_NAME, current_term.ADVISOR_ID, current_term.ADVISOR_NAME, current_term.DESCR2, current_term.SUBPLAN_DESCR, sgrp_with_date.STDGR_STDNT_GROUP, sgrp_with_date.STDGR_STDNT_GROUP_LDESC, current_term.STDNT_ENRL_STATUS, current_term_enrollment.ENRL_ADD_DT, sgrp_with_date.EFFDT, SI_Probation.SRVC_IND_CD, SI_Probation.SRVC_IND_REFRNCE, current_term.K_HOME_CAMPUS, previous_term.REQ_TERM, current_term.ACTION_DT, current_term.PROG_ACTION, current_term.STDNT_CAR_NBR
FROM (((((current_term LEFT JOIN previous_term ON current_term.EMPLID = previous_term.EMPLID) LEFT JOIN sgrp_with_date ON current_term.EMPLID = sgrp_with_date.EMPLID) LEFT JOIN SI_Probation ON current_term.EMPLID = SI_Probation.EMPLID) LEFT JOIN personal_email ON current_term.EMPLID = personal_email.EMPLID) LEFT JOIN current_term_enrollment ON current_term.EMPLID = current_term_enrollment.EMPLID) LEFT JOIN current_term_owtctr ON current_term.EMPLID = current_term_OWTCTR.EMPLID ORDER BY current_term_enrollment.ENRL_ADD_DT DESC;''')

exemptions = advising.CSVObject(envDict['exemptions'])

all_assignments = advisorSQLDB.export('all_assignments',where_statement="ORDER BY EMPLID") #list of list of items from table
untouched_advisors = envDict['UNTOUCHABLE_ADVISORS'].split(';')
all_assignments_filtered = [row for row in all_assignments if row[8] == 'E' and row[3] not in untouched_advisors]# and row[0] not in exemptions.csvData[0]]
advisor_counts = advisorSQLDB.export('advisor_assignment_count', where_statement='WHERE STDNT_ENRL_STATUS=Enrolled')

global_row_temp = '' #I'll need to have advisorCounting done using complexList, most likely. That'll be best. 
for row in all_assignments_filtered: 
    findCells = lambda line: (line[2],line[3],[line[4],line[5],line[6]]) 
    advisorCell,programCells = findCells(row) #uses lambda to grab relevant cells
    for items in exemptions.Data: #in the future, this should be done with the complexList object, or a method thereof
        if items[0] in row[0]: row.insert(0,f'Exception with: {items[3]} Reason: {items[4]}')
    global_row_temp = row[0]
    row.insert(0,advisorAPI.testProgramAdvisor(advisorCell,programCells))

mappedAdvisorCounting = lambda row: advisorAPI.incrementAdvisor(row[4])

header_list = all_assignments.pop(0)
unfiltered_file = advising.CSVObject(csvData=all_assignments,csvColumns=header_list)
unfiltered_file.export(f'{envDict["unfiltered_file"]}')
header_list.insert(0,"Advisor Suggestion:")
filtered_file = advising.CSVObject(csvData=all_assignments_filtered,csvColumns=header_list)
filtered_file.mapRows(mappedAdvisorCounting)
print(filtered_file.Columns)
filtered_file.export(f'{envDict["filtered_file"]}')

exit()