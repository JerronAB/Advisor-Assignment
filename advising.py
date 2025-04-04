class AdvisableSet: #functionality: store and maintain advisors and their programs; determine correct advisor/program
    def __init__(self, name=None) -> None:
        self.name = name
        self.Advisors = []
        self.Programs = []
        self.advisorCounts = {} #dictionary is my method for setting advisorcounts
    def __hash__(self) -> int: return hash(self.name)
    def __eq__(self, __value: str) -> bool: return __value == self.name
    def addAdvisors(self, advisors):
        if type(advisors) is list: [self.Advisors.append(adv) for adv in advisors]
        if type(advisors) is str: self.Advisors.append(advisors)
        for advisor in self.Advisors: #ran out of brain power. Make this better. 
            if advisor not in self.advisorCounts: self.advisorCounts[advisor] = 0
    def addPrograms(self, programs):
        if all(type(item) is list for item in programs): [self.Programs.append(item) for item in programs]
        if all(type(item) is str for item in programs): self.Programs.append(programs) 
    def testProgram(self, program): #returns true/false
        return program in self.Programs
    def testAdvisor(self, advisor):
        return advisor in self.Advisors
    def setAdvisorCount(self, advisor, number) -> None:
        if advisor not in self.Advisors: raise Exception(f'Advisor: {advisor} not in set: {self.name}')
        self.advisorCounts[advisor] = int(number)
    def incrementAdvisorCount(self,advisor) -> None:
        self.advisorCounts[advisor] +=1
        print(f'Advisor: {advisor} Count: {self.advisorCounts[advisor]}')
    def returnAdvisors(self) -> str: #study up on built-in dictionary methods for speed/efficiency
        testExist = lambda advisor: self.setAdvisorCount(advisor,0) if advisor not in self.AdvisorCounts else True
        map(testExist,self.Advisors)
        list_of_tuples = sorted(self.advisorCounts.items(),key=lambda x:x[1],) # dictionary.items() returns list of tuples; lambda is accessing second item in each tuple (the advisor count in this case)
        return ', '.join(['{0} ({1})'.format(advisorCount_tuple[0],advisorCount_tuple[1]) for advisorCount_tuple in list_of_tuples])
    def copy(self):
        newAdvisableSet = AdvisableSet(f'Copy of {self.name}')
        newAdvisableSet.Advisors = self.Advisors
        newAdvisableSet.Programs = self.Programs
        newAdvisableSet.advisorCounts = self.advisorCounts
        return newAdvisableSet

class AdvisorAPI: #this "API" maintains a dictionary of AdvisableSet instances, and coordinates info from tests.
    def __init__(self) -> None:
        self.AdvisableSets = set()
        #this lambda is an improvement, but only visually; ideally there would be a way to return value from set by hash. 
        self.extractSet = lambda programName: [advisable_set for advisable_set in self.AdvisableSets if programName == advisable_set][0]
    #BELOW are the methods for adding new sets/programs/advisors
    def newSet(self, name, advisors=None, programs=None):
        #should we return matching set instead of raising exception?
        if name in self.AdvisableSets: raise Exception("An advisable set with this name already exists.")
        newSet = AdvisableSet(name)
        if not advisors is None: newSet.addAdvisors(advisors)
        if not programs is None: newSet.addPrograms(programs)
        self.AdvisableSets.add(newSet)
    def addProgram(self,programName,programs):
        program = self.extractSet(programName)
        program.addPrograms(programs)
        self.AdvisableSets.add(program)
    def addAdvisors(self,programName,advisors):
        advSet = self.extractSet(programName)
        advSet.addAdvisors(advisors)
        self.AdvisableSets.add(advSet)

    #BELOW are the tests to run our advisors/programs against 
    def findSet(self, Program, advisor="") -> AdvisableSet:
        for set in self.AdvisableSets:
            #this is another version of self.extractSet, but testing program instead.
            if set.testProgram(Program): return set
            if set.testProgram([str(cell or '') for cell in Program]): return set
        #this runs separately so that every other set has a chance to test first:
        for set in self.AdvisableSets:
            Program[2] = ''
            if set.testProgram([str(cell or '') for cell in Program]):
                truncatedSet = set.copy()
                def testAdvisorModified(advisor): return False #always return false so we use our own string for returnAdvisors
                truncatedSet.testAdvisor = testAdvisorModified
                def returnAdvisorsModified():
                    #access current advisor and test if it matches provided set
                    return f"Without STGRP: {'CORRECT' if (advisor in truncatedSet.Advisors) else ', '.join(truncatedSet.Advisors)}"
                truncatedSet.returnAdvisors = returnAdvisorsModified
                return truncatedSet
        #we only get to this point if our search for advisablesets failed. 
        nullSet = AdvisableSet('nullSet') #if we don't find the program, above, create a "nullSet"; might be more efficient if this is global at the top?
        nullSet.addAdvisors('Exception: group not found')
        return nullSet
    def testProgramAdvisor(self, advisor, program) -> str:
        usableSet = self.findSet(program, advisor)
        if usableSet.testAdvisor(advisor): 
            return 'Correct'
        return usableSet.returnAdvisors()
    def setAdvisorCount(self, advisor, number):
        for advSet in self.AdvisableSets: #I can't think of an improvement that doesn't make this unreadable
            if advSet.testAdvisor(advisor): advSet.setAdvisorCount(advisor,number)

#SQLITE3 section
import sqlite3 as sqlt
class SQLInstance: #this will be less monstrous if we abstract it, too; use for error-checking and formatting? 
    def __init__(self, db_filename='file::memory:?cache=shared", &db') -> None: #CONTENT validation
        self.conn = sqlt.connect(db_filename) #do we want more features here? Like more complex table renaming?
        self.cursor = self.conn.cursor()
        self.dbname = db_filename
        self.tables = []
        self.valuesFormat = lambda items_list: ', '.join([f'\'{item}\'' for item in items_list])
        self.nameFormat = lambda table_name: table_name.replace(".csv","").replace(" ","_") #handles our table title renaming logic internally
    def addTable(self, table_name, headers_list):
        print(f'CREATE TABLE {self.nameFormat(table_name)} ({self.valuesFormat(headers_list)})')
        self.cursor.execute(f"CREATE TABLE {self.nameFormat(table_name)} ({self.valuesFormat(headers_list)})")
    def addRow(self, table_name, import_row):
        try:
            self.cursor.execute(f"INSERT INTO {self.nameFormat(table_name)} VALUES ({self.valuesFormat(import_row)});")
        except:
            self.addRow(table_name,[item.replace("\'","").replace("\"","") for item in import_row])
    def execCommand(self, command):
        self.cursor.execute(command)

class tableData:
    def __init__(self) -> None:
        self.Data = tuple()
        self.Columns = []
    def setData(self, data):
        print(f'setData running on iterable with {len(data)} items.')
        self.Data = tuple([item for item in data]) 
        self.integrityCheck()
    def addRow(self, line):
        self.Data += (line,)
    def addRows(self,rows):
        for row in rows: self.addRow(row)
        self.integrityCheck()
    def mapRows(self, mappedFunction, inPlace=False): #here, I need to explore using lambda & map, vs. using eval(), vs. using exec()
        print(f'Function being passed: {mappedFunction}')
        print(f'Modifying in-place: {inPlace}')
        if inPlace is True: self.Data = [mappedFunction(row) for row in self.Data if row is not None]
        else: return [mappedFunction(row) for row in self.Data if row is not None]
    def prune(self,testFunction,inPlace=True):
        #print([row for row in self.Data if testFunction(row)])
        if inPlace is True: self.Data = [row for row in self.Data if testFunction(row)]
        else: return [row for row in self.Data if testFunction(row)]
    def integrityCheck(self):
        print(f'Running integrity check---')
        if not all(isinstance(line, list) and len(line) == len(self.Columns) for line in self.Data): raise TypeError("tableData data must be a tuple of lists with the same length as tableColumns")
        print('Passed.')
    def deDup(self, dedupColumn,sortFx=None): #make this nicer and cleaner
        if sortFx is not None:
            self.Data = sorted(self.Data,key=sortFx,reverse=True) #this is not fully featured, I'm throwing it in to fix something. 
        index = self.Columns.index(dedupColumn)
        dedupping_set = set()
        dedupped_list = [item for item in self.Data if item[index] not in dedupping_set and not dedupping_set.add(item[index])] #set() does not allow duplicate values. Here we add all items to a set on each loop, and stop loop if item is in set already
        self.Data = ()
        self.setData(dedupped_list)
    def export(self,filename,exportColumns=True,ignoreExistingFiles=False,delimiter_str=',',formatPRN=False): #looking at export function to make sure it doesn't export empty cells
        nonetoString = lambda cells: [str(cell or '') for cell in cells]
        if formatPRN:
            fillDigits = lambda string: f'{'0' * (9-len(string))}{string}'
            prnFilename = filename.replace('.csv','.prn')
            print(f'Writing to... {prnFilename}')
            with open(prnFilename,'w',newline='') as prnFile:
                prnString = ''
                for row in self.Data:
                    idList = [fillDigits(id) for id in row]
                    prnString += ' '.join(idList)
                    prnString += '\n'
                prnFile.write(prnString)
        else:
            #this uses the 'writer' function from the csv module
            print(f'Writing to... {filename}')
            def writeOut():
                with open(filename, 'w', newline='') as csv_file:
                    if delimiter_str == '\t':
                        csvWriter = writer(csv_file, dialect="excel-tab")
                    elif delimiter_str == ',':
                        csvWriter = writer(csv_file, delimiter=delimiter_str)
                    if exportColumns: csvWriter.writerow(nonetoString(self.Columns))
                    [csvWriter.writerow(nonetoString(row)) for row in self.Data]
            try: 
                #bare with me; if this successfully reads from the file, we know it's there and don't want to overwrite it, so we raise error
                #If we CAN'T find it, we know we can export the file fine. 
                with open(filename, 'r'): raise FileExistsError()
                #This avoids needing to import the os module; I'm open to suggestions here though
            except FileNotFoundError:
                writeOut()
            except FileExistsError:
                if ignoreExistingFiles: writeOut()
                else: raise FileExistsError(f'Flag ignoreExistingFiles set to False. The file {filename} already exists, so it will not be overwritten. Call script with "--ignore-existing" to bypass.')
                

class SQLTableSubclass(tableData):
    def __init__(self, db_tablename=None,db_filename="../temp.db") -> None:
        tableData.__init__(self)
        self.instance = SQLInstance(db_filename)
        self.dbname = db_filename
        self.table = db_tablename
        self.displaced_row = 0
    def execute(self, command):
        self.instance.cursor.execute(command)
    def dataPush(self):
        print(f'Initiating dataPush to SQL... Table: {self.table}')
        self.instance.addTable(self.table,self.Columns)
        for row in self.Data[self.displaced_row:]:
            self.instance.addRow(self.table,row)
            #self.displaced_row += 1 #count number of loops and change this variable upon exit? trying to think of better ways here; leaving this for now
        self.instance.conn.commit()
        self.instance.conn.close()
    def dataPull(self,select_statement=None):
        print(f'Initiating data pull... ')
        self.Columns = [header[1] for header in self.instance.cursor.execute(f'PRAGMA table_info ({self.table})')]
        if select_statement is None: self.setData([list(item) for item in self.instance.cursor.execute(f'SELECT * FROM {self.table}')])
        else: self.setData([list(item) for item in self.instance.cursor.execute(select_statement)])

from csv import reader,writer #maybe this can just become a couple methods on the tableData class??
import pickle
from hashlib import md5
class CSVTableSubclass(tableData): #creates and interacts with tableData object
    def __init__(self, filename=None, skipPkl=False) -> None:
        tableData.__init__(self)
        if filename is not None: self.fileIntake(filename,skipPickle=skipPkl)
    def fileIntake(self, filename, skipPickle=False):
        self.name = filename.split('\\')
        self.filename = filename
        self.name = self.name[-1]
        self.pklFile = filename.replace(".csv",".pkl")
        try:
            if skipPickle: raise LookupError
            #instead of doing nested if/else statements, try/except seemed better. Opinions welcome
            pkl = pickle.load(open(self.pklFile,'rb')) #fails and moves on if there's not a pkl file
            print(f"Stored bytecode found from previous run; testing checksum for {self.name}...")
            if pkl['checksum'] == md5(open(filename, 'rb').read()).hexdigest(): 
                print('Checksums are the same! Importing data directly from bytecode.')
                self.Columns = pkl['columns']
                self.Data = pkl['data']
                self.integrityCheck()
            else: 
                print('Checksums are not the same; importing CSV,')
                raise Exception('Checksums are not the same; importing CSV.') #fails and moves on if checksums don't match
        except:
            with open(filename, 'r', encoding='ISO-8859-1') as csvfile: #this encoding makes Excel-exported CSV files readable
                if ".tsv" in filename: #for tsv files
                    self.name = self.name.replace('.tsv','')
                    csvData = [row for row in reader(csvfile, delimiter="\t", quotechar='"')]
                else: #this is for csv files
                    self.name = self.name.replace('.csv','')
                    csvData = [row for row in reader(csvfile)]
                self.Columns = list(map(lambda input_str: input_str.replace("ï»¿",""), csvData.pop(0)))
                self.setData(csvData)
                if not skipPickle:
                    print(f"Dumping new data into pkl file: {self.pklFile}.")
                    pklDump = dict()
                    pklDump['checksum'] = md5(open(filename, 'rb').read()).hexdigest()
                    pklDump['data'] = self.Data
                    pklDump['columns'] = self.Columns
                    pickle.dump(pklDump, open(self.pklFile,'wb'))

def migrateData(source,target_class):
    print(f'Migrating data. Type of source: {type(source)}')
    if type(source) is SQLTableSubclass and target_class.lower() in ["csv","csvclass","csvobject"]: #we can probably reduce these to just 2 if-statements since we use the same underlying class
        CSVTable = CSVTableSubclass()
        CSVTable.Columns = source.Columns
        CSVTable.Data = source.Data
        return CSVTable
    if type(source) is tableData and target_class.lower() in ["csv","csvclass","csvobject"]:
        CSVTable = CSVTableSubclass()
        CSVTable.Columns = source.Columns
        CSVTable.Data = source.Data
        return CSVTable
    if type(source) is CSVTableSubclass and target_class.lower() in ["sql","sqltablesubclass","sqlclass"]:
        fileName = source.filename.replace('\\',';').replace('/',';')
        namesplit = fileName.split(';')
        fileName = namesplit[-1]
        print(f'Filename of CSV file: {fileName}')
        SQLTable = SQLTableSubclass(db_tablename=fileName)
        SQLTable.Columns = source.Columns
        SQLTable.Data = source.Data
        return SQLTable
    if type(source) is tableData and target_class.lower() in ["sql","sqltablesubclass","sqlclass"]: #this will break because source won't have a filename attribute; don't need it for now tho
        fileName = source.filename.replace('\\',';').replace('/',';')
        namesplit = fileName.split(';')
        fileName = namesplit[-1]
        print(f'Filename of CSV file: {fileName}')
        SQLTable = SQLTableSubclass(db_tablename=fileName)
        SQLTable.Columns = source.Columns
        SQLTable.Data = source.Data
        return SQLTable