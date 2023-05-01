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

adv_counts = advisorSQLDB.export('advisor_assignment_count', where_statement='WHERE STDNT_ENRL_STATUS = \'Enrolled\'') #this stanza shows us our issues with complexList right now
for item in adv_counts.Data:
    advisorAPI.setAdvisorCount(item[0],int(item[1]))

advisorSQLDB.arbitraryExecute('''CREATE TABLE all_assignments AS SELECT current_term.EMPLID, current_term.STUDENT_NAME, current_term.ADVISOR_ID, current_term.ADVISOR_NAME, current_term.DESCR2, current_term.SUBPLAN_DESCR, sgrp_with_date.STDGR_STDNT_GROUP, sgrp_with_date.STDGR_STDNT_GROUP_LDESC, current_term.STDNT_ENRL_STATUS, current_term_enrollment.ENRL_ADD_DT, sgrp_with_date.EFFDT, SI_Probation.SRVC_IND_CD, SI_Probation.SRVC_IND_REFRNCE, current_term.K_HOME_CAMPUS, previous_term.REQ_TERM, current_term.ACTION_DT, current_term.PROG_ACTION, current_term.STDNT_CAR_NBR
FROM (((((current_term LEFT JOIN previous_term ON current_term.EMPLID = previous_term.EMPLID) LEFT JOIN sgrp_with_date ON current_term.EMPLID = sgrp_with_date.EMPLID) LEFT JOIN SI_Probation ON current_term.EMPLID = SI_Probation.EMPLID) LEFT JOIN personal_email ON current_term.EMPLID = personal_email.EMPLID) LEFT JOIN current_term_enrollment ON current_term.EMPLID = current_term_enrollment.EMPLID) LEFT JOIN current_term_owtctr ON current_term.EMPLID = current_term_OWTCTR.EMPLID ORDER BY current_term_enrollment.ENRL_ADD_DT DESC;''')

exemptions = advising.CSVObject(envDict['exemptions'])
exempt_dict = {int(row[0]): row for row in exemptions.Data}

all_assignments = advisorSQLDB.export('all_assignments',where_statement="ORDER BY EMPLID",complexlist=False) #list of list of items from table
untouched_advisors = envDict['UNTOUCHABLE_ADVISORS'].split(';')
all_assignments_filtered = [row for row in all_assignments if row[8] == 'E' and row[3] not in untouched_advisors]

#this can be much more efficient, but for right now I don't care. 
for row in all_assignments_filtered: #eventually use mapRows for this
    findCells = lambda line: (line[3],[line[4],line[5],line[6]]) 
    advisorCell,programCells = findCells(row) #uses lambda to grab relevant cells
    suggestCell = advisorAPI.testProgramAdvisor(advisorCell,programCells)
    try: 
        suggestCell=f'Exemption with: {exempt_dict[int(row[0])][3]} Reason: {exempt_dict[int(row[0])][4]}' 
    except: 
        pass
    row.insert(0,suggestCell)

header_list = all_assignments.pop(0)
advising.CSVObject(csvData=all_assignments,csvColumns=header_list).export(f'{envDict["unfiltered_file"]}')
header_list.insert(0,"Advisor Suggestion:")
filtered_export = advising.CSVObject(csvData=all_assignments_filtered,csvColumns=header_list)
filtered_export.deDup('EMPLID')
filtered_export.export(f'{envDict["filtered_file"]}')

exit()