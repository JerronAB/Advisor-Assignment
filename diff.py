#differencing for identifying changes over time

#a few ways to do this:
#just test ID's against each other from the unfiltered CSV

#hashes are also an option because it's cool

#should use "advising.py" for data intake and maybe information handling

from advising import CSVObject

envDict = {}
with open('.env') as envFile:
    for line in envFile:
        key, value = line.strip().split('=')
        envDict[key.strip()] = value.strip()
[print(f'{key}: {envDict[key]}') for key in envDict]

latest_assignments = CSVObject(envDict["latest_assignments"])
historical_assignments = CSVObject(envDict["historical_assignments"])

#how do we want to be tracking out data in historical data? Do we want one-at-a-time? What about a straight-up git-repo? 
#If git repo, we can have 3 values: enrollment, STD_ID/STDNAME, and ADV_ID/ADVNAME -- Keep one student ID at any given time (maybe best to do that using the "current term.csv" file)
#Sort by STDID
#This way, we have long-term change stats, with visuals. 
#After we get all this data in place and sorted, is it best just to use a bash-script essentially? For the git differencing part? How else can we do this?

#Another option: simply notating change. Go line-by-line on existing file. Test student. If there's something new, just add that on the next line of the CSV. 
#This might be the best and least complicated way to do this. 