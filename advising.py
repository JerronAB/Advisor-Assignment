#Use map more often than list generators
#CSV Data exports directly--line by line--into SQL, so a middle-man isn't necessarily advantageous. 
#Where should my exception management occur? Where should my deduping occur?

class AdvisableSet: #functionality: store and maintain advisors and their programs; use function to determine if program or advisor is present
    def __init__(self, name=None) -> None:
        self.name = name
        self.Advisors = []
        self.Programs = []
        self.advisorCounts = {} #dictionary is my method for setting advisorcounts. It only accesses whenever called, which is good, but is this really the best way? Doesn't matter much because abstractions :)
    def addAdvisors(self, advisors):
        if type(advisors) is list: [self.Advisors.append(adv) for adv in advisors]
        if type(advisors) is str: self.Advisors.append(advisors)
        for advisor in self.Advisors: #ran out of brain power. Make this better. 
            if advisor not in self.advisorCounts: self.advisorCounts[advisor] = 0
    def addPrograms(self, programs):
        if all(type(item) is list for item in programs): [self.Programs.append(item) for item in programs]
        if all(type(item) is str for item in programs): self.Programs.append(programs) 
    def testProgram(self, program):
        return True if program in self.Programs else False
    def testAdvisor(self, advisor):
        return True if advisor in self.Advisors else False
    def setAdvisorCount(self, advisor, number) -> None:
        if advisor not in self.Advisors: raise Exception(f'Advisor: {advisor} not in set: {self.name}')
        self.advisorCounts[advisor] = int(number)
    def incrementAdvisorCount(self,advisor) -> None:
        self.advisorCounts[advisor] +=1
    def returnAdvisors(self) -> str: #study up on built-in dictionary methods for speed/efficiency
        testExist = lambda advisor: self.AdvisorCount(advisor,0) if advisor not in self.AdvisorCounts else True
        map(testExist,self.Advisors)
        list_of_tuples = sorted(self.advisorCounts.items(),key=lambda x:x[1],)[:3] # dictionary.items() returns list of tuples; lambda is accessing second item in each tuple (the advisor count in this case)
        return ', '.join(['{0} ({1})'.format(advisorCount_tuple[0],advisorCount_tuple[1]) for advisorCount_tuple in list_of_tuples][:3])

class AdvisorAPI: #this "API" maintains a dictionary of AdvisableSet instances, and coordinates info from tests.
    def __init__(self) -> None:
        self.AdvisableSets = {}
    def newSet(self, name, advisors=None, programs=None):
        if name in self.AdvisableSets: raise Exception("An advisable set with this name already exists.")
        newSet = AdvisableSet(name)
        if not advisors is None: newSet.addAdvisors(advisors)
        if not programs is None: newSet.addPrograms(programs)
        self.AdvisableSets[name] = newSet
    def addProgram(self,name,programs):
        if name not in self.AdvisableSets: raise Exception(f"The set {name} does not exist")
        self.AdvisableSets[name].addPrograms(programs)
    def addAdvisors(self,name,advisors):
        if name not in self.AdvisableSets: raise Exception(f"The set {name} does not exist")
        self.AdvisableSets[name].addAdvisors(advisors)
    def findSet(self, Program) -> AdvisableSet:
        for set in self.AdvisableSets:
          if self.AdvisableSets[set].testProgram(Program): return self.AdvisableSets[set] #I think there's a more efficient, built-in way to iterate through these
          if self.AdvisableSets[set].testProgram([str(cell or '') for cell in Program]): return self.AdvisableSets[set]
        nullSet = AdvisableSet('nullSet') #if we don't find the program, above, create a "nullSet"; might be more efficient if this is global at the top?
        nullSet.addAdvisors('Exception: group not found')
        return nullSet
    def testProgramAdvisor(self, advisor, program) -> str: #two-steps here, possibly not efficient. First, find program. Second, match Advisor against program. Can we combine that into one big operation? Or is this easier?
        usableSet = self.findSet(program)
        if usableSet.testAdvisor(advisor): #it's gettin' messy; I'm using try/except to catch when there's no program. Can I do this in findSet instead? Yes. But that won't return an advisablesets object. 
            return 'Correct'
        else:
            return usableSet.returnAdvisors()
    def incrementAdvisor(self, advisor): #GETTIN MESSY AGAIN
        for set in self.AdvisableSets:
            if self.AdvisableSets[set].testAdvisor(advisor): self.AdvisableSets[set].incrementAdvisorCount(advisor)
    def setAdvisorCount(self, advisor, number):
        for set in self.AdvisableSets:
            if self.AdvisableSets[set].testAdvisor(advisor): self.AdvisableSets[set].setAdvisorCount(advisor, number)
#not sure where I'll be adding in exception management (counseling or advexemption list) or advisor counting; locus in the CSV import?

#SQLITE3 section
import sqlite3 as sqlt
class SQLInstance: #this will be less monstrous if we abstract it, too; use for error-checking and formatting? 
    def __init__(self, db_filename=':memory:') -> None: #CONTENT validation
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

class SQLAPI: #COMMAND validation
    def __init__(self, db_filename=':memory:') -> None:
        self.instance = SQLInstance(db_filename)
        self.dbname = db_filename
        self.tables = []
    def addTable(self, table_name, headers_list):
        self.tables.append(table_name)
        self.instance.addTable(table_name,headers_list)
    def addRow(self, table_name, import_row):
        if not type(import_row) is list: raise TypeError('import_row Must be list.')
        self.instance.addRow(table_name,import_row)
    def addRows(self, table_name, rows, headers_list=[]):
        if not all(type(item) is list for item in rows): raise TypeError('rows must be a list of lists. ')
        if not type(headers_list) is list: raise TypeError('headers_list Must be a list. ')
        if table_name not in self.tables: self.addTable(table_name,headers_list)
        [self.addRow(table_name,row) for row in rows] #gotta be a more efficient way to do this; map maybe?
    def arbitraryExecute(self, command):
        self.instance.cursor.execute(command)
    def export(self, table_name, select_statement='*',where_statement=''): #this is currently limited to just exporting a table
        table_name = self.instance.nameFormat(table_name)
        header_list = [header[1] for header in self.instance.cursor.execute(f'PRAGMA table_info ({table_name})')]
        data_to_export = [list(item) for item in self.instance.cursor.execute(f'SELECT {select_statement} FROM {table_name} {where_statement}')] #I think I'm performing unnecessary operations here; can be paired down to a simple list comp
        data_to_export.insert(0,header_list)
        return data_to_export

class complexList:
    def __init__(self) -> None:
        self.Data = []
        self.Columns = []
    def addData(self, data):
        try:
            self.Data = [item for item in data]
            self.integrityCheck()
        except:
            self.Data = data
            self.integrityCheck()
    def mapRows(self, mappedFunction, inPlace=False):
        print(f'Function being passed: {mappedFunction}')
        print(f'Modifying in-place: {inPlace}')
        if inPlace is True: self.Data = [mappedFunction(row) for row in self.Data if row is not None]
        else: return [mappedFunction(row) for row in self.Data if row is not None]
    def integrityCheck(self):
        if not all(type(line) is list for line in self.Data): raise TypeError("complexList data must be a list of lists.")
    def deDup(self, dedupColumn,sortFunc=None):
        if sortFunc is None: 
            pass
        else:
            pass

from csv import reader,writer
class CSVObject: #creates and interacts with complexList object
    def __init__(self, filename=None,csvData=None,csvColumns=None) -> None:
        self.new_complexList = complexList()
        if filename is not None: self.fileIntake(filename)
        else:
            self.new_complexList.Columns=(csvColumns)
            self.new_complexList.addData(csvData)
        self.Data = self.new_complexList.Data
        self.Columns = self.new_complexList.Columns
    def fileIntake(self, filename):
        with open(filename, 'r', encoding='ISO-8859-1') as csvfile: #UTF-8
                self.filename = filename
                newname = filename.split('\\')
                self.name = newname[-1].replace('.csv','')
                csvData = [row for row in reader(csvfile)]
                self.new_complexList.Columns = list(map(lambda input_str: input_str.replace("ï»¿",""), csvData.pop(0)))
                self.new_complexList.addData(csvData)
    def mapRows(self, mappedFunction, changeInPlace=False):
        if changeInPlace is True: self.new_complexList.mapRows(mappedFunction,inPlace=changeInPlace)
        else: return self.new_complexList.mapRows(mappedFunction,inPlace=changeInPlace)
    def mappedExport(self, mappedFunction,exportFile=None):
        self.new_complexList.mapRows(mappedFunction=mappedFunction,inPlace=True)
        if exportFile is not None: self.export(exportFile)
    def export(self,filename): 
        with open(filename,'w',newline='') as csv_file:
            my_writer = writer(csv_file, delimiter = ',')
            self.new_complexList.Data.insert(0,self.new_complexList.Columns)
            for row in self.new_complexList.Data:
                my_writer.writerow(row)