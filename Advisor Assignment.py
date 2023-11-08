#NEXT GOAL: can the flushing of the .db file be modified for speed improvements? The current default buffer size is ~8000
#import time
#starttime = time.time()
import advising

envDict = {}
with open('.env') as envFile:
    from os import path
    homepath = path.expanduser('~')
    for line in envFile:
        if line[0] != "#": key, value = line.strip().split('=')
        value = value.replace('~',homepath)
        envDict[key.strip()] = value.strip()

[print(f'{key}: {envDict[key]}') for key in envDict]

def removeOldDB():
    #workaround to remove temp.db file
    try:
        from os import remove
        remove(envDict['db_file'])
    except:
        pass
removeOldDB()

#this first portion checks to see if this program has already been run, and if Advisor Assignment has already been done. 

def moveAdvisorFiles(fileRoot): #this might have to run first and exit if conditions are met
    from sys import setrecursionlimit
    setrecursionlimit(10) #doing this because on accidental recursion error, I could DDOS the fileserver
    print(f'\'{envDict["peoplesoft_file"]}\' file found, copying files to storage: {envDict["storage_root"]}')
    from os import path, makedirs, system
    simple_dates = {i:'th' for i in range(32) if str(i)[-1] == '0'} #if last digit in 
    simple_dates.update({i:'st' for i in range(32) if str(i)[-1] == '1'})
    simple_dates.update({i:'nd' for i in range(32) if str(i)[-1] == '2'})
    simple_dates.update({i:'rd' for i in range(32) if str(i)[-1] == '3'})
    simple_dates.update({i:'th' for i in range(32) if str(i)[-1] in ('4','5','6','7','8','9')})
    for i in ('11','12','13'): simple_dates[i] = 'th'
    fileserver = fileRoot
    #creating the folders
    from datetime import datetime
    month_year = datetime.now().strftime("%B %Y")
    today = datetime.now().strftime("%d")
    print('Testing for existing path: {}{}'.format(fileserver,month_year))
    if not path.exists(f'{fileserver}{month_year}'): makedirs('{}{}'.format(fileserver,month_year))
    def addToday(iteration=1): #making this a function so it can be recursive
        try:
            global folder #I do this so we can access it later
            if iteration == 1: folder = f'{fileserver}{month_year}/{today}{simple_dates[int(today)]}/'
            else: folder = f'{fileserver}{month_year}/{today}{simple_dates[int(today)]} - {iteration}{simple_dates[iteration]} run/'
            print(f'{folder}')
            makedirs(f'{folder}')
        except FileExistsError:
            iteration += 1
            addToday(iteration)
        except:
            print('ANOTHER ERROR HAS OCCURRED. ')
    addToday()
    #now copying the files, except for finalized temp.csv
    def copyFile(source):
        print('copy "{}" "{}"'.format(source.replace("/","\\"),folder.replace("/","\\"))) #f-strings don't allow backslashes in expressions
        system('copy "{}" "{}"'.format(source.replace("/","\\"),folder.replace("/","\\")))
    for file in envDict['import_csvs'].split(','): copyFile(file)
    copyFile(envDict['peoplesoft_file'])
    copyFile(envDict['filtered_file'])
    copyFile(envDict['unfiltered_file'])
    exit()

if path.exists(envDict['peoplesoft_file']): moveAdvisorFiles(envDict['storage_root'])

def finalizeAdvChanges(filename, outputFilename, columns=False): #takes all rows w/ empty ID's and creates a new file from them
    print('Potentially completed advisor assignment file found...') 
    from os import path
    if path.exists(outputFilename): raise FileExistsError(f'{outputFilename} already exists and will not be overwritten.')
    completedList = advising.CSVTableSubclass(filename,skipPkl=True)
    #the next 2 lines remove any rows that DON'T have an empty advisor ID
    emptyAdvID = lambda row: row[3] == ''
    completedList.prune(emptyAdvID)
    if len(completedList.Data) != 0:
        IDList = advising.CSVTableSubclass(envDict['advisor_list'])
        def IDColumns(row): 
            #this is an inefficient lookup process, but it shouldn't matter
            #later I'll make the advID from advisorList searchable first
            advID = f'FIND ID MANUALLY: {row[4]}'
            for sublist in IDList.Data:
                if sublist[1] == row[4]: 
                    advID = sublist[0] #returning the ID in adivsorList if the name matches
                    break
            if columns == True: return [row[1],row[2],advID,row[4],row[5],row[7],row[8]] 
            return [row[1],advID]
        completedList.mapRows(IDColumns,True)
        completedList.Columns = [completedList.Columns[index] for index in [1,2,3,4,5,7,8]] #change out columns to match...
        completedList.export(outputFilename, exportColumns=columns) #the data this object is exporting 

if path.exists(envDict['filtered_file']) and not path.exists(envDict['peoplesoft_file']):
    finalizeAdvChanges(envDict['filtered_file'],envDict['peoplesoft_file'])
    finalizeAdvChanges(envDict['filtered_file'],envDict['email_file'],columns=True) 
    moveAdvisorFiles(envDict['storage_root'])
    exit()

#The actual advisor assignment occurs here. 
advisorAPI = advising.AdvisorAPI()
print('Importing advisorList and associating advisors...')
AdvisorList = advising.CSVTableSubclass(envDict['advisor_list'])
@AdvisorList.mapRows
def advisorDecision(row):
    if row[0] != '' and row[1] == '' and row[2] == '':
        advisorAPI.latestProgramName = row[0] #doing this so that I don't have to initialize an unnecessary global var. Instead, just mapping a new attribute to our instance. 
        advisorAPI.newSet(row[0])
    elif row[1] != '':
        advisorAPI.addAdvisors(advisorAPI.latestProgramName,row[1])
    if row[2] != '':
        advisorAPI.addProgram(advisorAPI.latestProgramName,[row[2],row[3],row[4]])

print('Importing CSV\'s...')
importableCSVs = envDict['import_csvs'].split(',')
for filename in importableCSVs: #can't seem to use this w/ lambda and map
    csvObj = advising.CSVTableSubclass(filename)
    filename = filename.split('/') #this dedupping is not fully featured, I'm just throwing it in to fix something
    filename = filename[-1]
    if filename == "SGRP with date.csv": csvObj.deDup("EMPLID",lambda x: x[-1]) #orders everything by date, then dedups
    SQLTable = advising.migrateData(csvObj,"sql")
    SQLTable.dataPush()

print('Importing and setting advisor count...')
adv_counts = advising.SQLTableSubclass(db_tablename='advisor_assignment_count',db_filename=envDict['db_file'])
adv_counts.dataPull('SELECT * FROM advisor_assignment_count WHERE STDNT_ENRL_STATUS = \'Enrolled\'')
[advisorAPI.setAdvisorCount(item[0],int(item[1])) for item in adv_counts.Data]
print('Creating new table...')
#"execute" can be called by any available SQLTableSubclass instance; I used one from above to make things slightly faster
adv_counts.execute('''CREATE TABLE all_assignments AS SELECT current_term.EMPLID, current_term.STUDENT_NAME, current_term.ADVISOR_ID, current_term.ADVISOR_NAME, current_term.DESCR2, current_term.SUBPLAN_DESCR, sgrp_with_date.STDGR_STDNT_GROUP, sgrp_with_date.STDGR_STDNT_GROUP_LDESC, current_term.STDNT_ENRL_STATUS, current_term_enrollment.ENRL_ADD_DT, sgrp_with_date.EFFDT, SI_Probation.SRVC_IND_CD, SI_Probation.SRVC_IND_REFRNCE, current_term.K_HOME_CAMPUS, previous_term.REQ_TERM, current_term.ACTION_DT, current_term.PROG_ACTION, current_term.STDNT_CAR_NBR
FROM (((((current_term LEFT JOIN previous_term ON current_term.EMPLID = previous_term.EMPLID) LEFT JOIN sgrp_with_date ON current_term.EMPLID = sgrp_with_date.EMPLID) LEFT JOIN SI_Probation ON current_term.EMPLID = SI_Probation.EMPLID) LEFT JOIN personal_email ON current_term.EMPLID = personal_email.EMPLID) LEFT JOIN current_term_enrollment ON current_term.EMPLID = current_term_enrollment.EMPLID) LEFT JOIN current_term_owtctr ON current_term.EMPLID = current_term_OWTCTR.EMPLID ORDER BY current_term_enrollment.ENRL_ADD_DT DESC;''')

print('Grabbing and setting dictionary for exemptions...')
exemptions = advising.CSVTableSubclass(envDict['exemptions'])
exempt_dict = {int(row[0]): row for row in exemptions.Data} #row[0] is the student ID

print('Generating all_assignments as an ordered, unfiltered list.')
all_assignments = advising.SQLTableSubclass(db_tablename='all_assignments',db_filename=envDict['db_file']) #instance of class attached to all_assignments table
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

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--ignore-existing', action='store_true', help='Ignore existing files')
args = parser.parse_args()
ignoreExistingFiles = args.ignore_existing

print('Exporting unfiltered advisor assignment file...')
def BlankFirst(single_row):
    if single_row[0].isnumeric(): single_row.insert(0,'Skipped')
    return single_row
all_assignments.mapRows(BlankFirst,inPlace=True)
all_assignments.export(f'{envDict["unfiltered_file"]}',ignoreExistingFiles=ignoreExistingFiles)

print('Exporting filtered advisor assignment file...')
filtered_export = advising.CSVTableSubclass()
filtered_export.Columns = header_list
filtered_export.setData(all_assignments_filtered)
filtered_export.deDup('EMPLID')
filtered_export.export(f'{envDict["filtered_file"]}',ignoreExistingFiles=ignoreExistingFiles)

#endtime = time.time()
#executiontime = endtime - starttime
#print(f'SCRIPT EXECUTION TIME MAIN BRANCH: {executiontime:.4f} seconds')

exit()
