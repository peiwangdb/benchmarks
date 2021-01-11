import pandas as pd

master_df = pd.read_csv("openlookeng_results_master.csv")
mybranch_df = pd.read_csv("openlookeng_results.csv")
masterMap = {}
mybranchMap = {}
master_res = master_df.filter(regex=("query|elapsedTime.*")).to_numpy()
mybranch_res = mybranch_df.filter(regex=("query|elapsedTime.*")).to_numpy()
for row in master_res:
    if row[0] not in masterMap:
        masterMap[row[0]] = [row[1]]
    else:
        masterMap[row[0]].append(row[1])
for row in mybranch_res:
    if row[0] not in mybranchMap:
        mybranchMap[row[0]] = [row[1]]
    else:
        mybranchMap[row[0]].append(row[1]) 
result_rows = []
result = pd.DataFrame(columns=('query', 'elapsedTime(master)', 'elapsedTime(mybranch)'))
for key, value in masterMap.items():
   resultDict = {'query': None, 'elapsedTime(master)': None, 'elapsedTime(mybranch)': None}
   masterTimes = sorted(value)
   mybranchTimes = sorted(mybranchMap[key])
   resultDict['query'] = key
   resultDict['elapsedTime(master)'] = masterTimes[0]
   resultDict['elapsedTime(mybranch)'] = mybranchTimes[0]
   result_rows.append(resultDict)
result = pd.DataFrame(result_rows)
pd.set_option('display.max_rows', None)
print(result)
    
    
