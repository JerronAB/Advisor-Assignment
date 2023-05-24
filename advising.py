#Goals:
#Use map more often than list generators
#use composition from builtin python classes

class AdvisableSet: #functionality: store and maintain advisors and their programs; determine correct advisor/program
    def __init__(self, name=None) -> None:
        self.name = name
        self.Advisors = []
        self.Programs = []
        self.advisorCounts = {} #dictionary is my method for setting advisorcounts
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
        print(f'Advisor: {advisor} Count: {self.advisorCounts[advisor]}')
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

class tableData:
    def __init__(self) -> None:
        self.Data = tuple()
        self.Columns = []
    def setData(self, data):
        print(f'setData running on iterable with {len(data)} items.')
        generator_object = (item for item in data) #apparently the generator object itself must be iterated through
        print('generator_object created. Running tuple append')
        for line in generator_object: self.Data += (line,) #calling this function over and over again may result in slowdown
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
    def integrityCheck(self):
        print(f'Running integrity check---')
        if not all(isinstance(line, list) and len(line) == len(self.Columns) for line in self.Data): raise TypeError("tableData data must be a tuple of lists with the same length as tableColumns")
        print('Passed.')
    def deDup(self, dedupColumn): #make this nicer and cleaner
        index = self.Columns.index(dedupColumn)
        dedupping_set = set()
        dedupped_list = [item for item in self.Data if item[index] not in dedupping_set and not dedupping_set.add(item[index])] #set() does not allow duplicate values. Here we add all items to a set on each loop, and stop loop if item is in set already
        self.Data = ()
        self.setData(dedupped_list)
    def export(self,filename): #looking at export function to make sure it doesn't export empty cells
        #this uses the 'writer' function from the csv module
        nonetoString = lambda cells: [str(cell or '') for cell in cells]
        print(f'Writing to... {filename}')
        with open(filename,'w',newline='') as csv_file:
            my_writer = writer(csv_file, delimiter = ',')
            my_writer.writerow(nonetoString(self.Columns))
            for row in self.Data:
                my_writer.writerow(nonetoString(row))

#QUICK TO DO LIST: make sure this class can maintain rows properly then perform dataSync; remember to use tableData's methods since it's the superclass
class SQLTableSubclass(tableData):
    def __init__(self, db_tablename=None,db_filename='test.db') -> None:
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
        if select_statement is None:
            self.Columns = [header[1] for header in self.instance.cursor.execute(f'PRAGMA table_info ({self.table})')]
            self.setData([list(item) for item in self.instance.cursor.execute(f'SELECT * FROM {self.table}')])
        else:
            self.Columns = [header[1] for header in self.instance.cursor.execute(f'PRAGMA table_info ({self.table})')]
            self.setData([list(item) for item in self.instance.cursor.execute(select_statement)])

from csv import reader,writer #maybe this can just become a couple methods on the tableData class??
class CSVTableSubclass(tableData): #creates and interacts with tableData object
    def __init__(self, filename=None) -> None:
        tableData.__init__(self)
        if filename is not None: self.fileIntake(filename)
    def fileIntake(self, filename):
        with open(filename, 'r', encoding='ISO-8859-1') as csvfile: #UTF-8
                self.filename = filename
                newname = filename.split('\\')
                self.name = newname[-1].replace('.csv','')
                csvData = [row for row in reader(csvfile)]
                self.Columns = list(map(lambda input_str: input_str.replace("ï»¿",""), csvData.pop(0)))
                self.setData(csvData)

def migrateData(source,target_class):
    print(f'Type of source: {type(source)}')
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