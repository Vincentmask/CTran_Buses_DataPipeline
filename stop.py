import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import seaborn as sns 
from urllib.request import urlopen
from bs4 import BeautifulSoup
import bs4 
import re
import json
from datetime import datetime

url = "http://www.psudataeng.com:8000/getStopEvents/"
html = urlopen(url)

soup = BeautifulSoup(html, 'lxml')
bs4.BeautifulSoup
text = soup.get_text()
now = datetime.now()
d = now.strftime("%m-%d-%y")
fname = d + 'stop.json'

# *** find and display every stop events ID ***
soup.find_all('h3')
stop_ids = soup.find_all("h3")
all_id = []
for id in stop_ids:
  lambda_obj = lambda x: (x is not None)
  temp = list(filter(lambda_obj, id))
  for x in temp:
    for text in x.split():
      if text.isdigit():
        all_id.append(text)

# *** find and display every row of dataset ***
rows = soup.find_all('tr')
list_rows = []
i = -1
for row in rows:
  cells = row.find_all('td')
  if str(cells) == ("[]"):
    i += 1
    continue 
  cells = str(cells) + ", " + str(all_id[i])
  str_cells = str(cells)
  clean = re.compile('<.*?>')
  clean2 = (re.sub(clean, '', str_cells))
  list_rows.append(clean2)

 

df = pd.DataFrame(list_rows)
df1 = df[0].str.split(',', expand=True)
df1[0] = df1[0].str.strip('[')
df1[22] = df1[22].str.strip(']')

col_labels = ["vehicle_number", "leave_time", "train", "route_number",
                "direction", "service_key", "stop_time", "arrive_time", "dwell", "location_id",
                "door", "lift", "ones", "offs", "estimated_load", "maximum_speed", "train_mileage", 
                "pattern_distance", "location_distance", "x_coordinate", "y_coordinate", 
                              "data_source", "schedule_status", "trip_id"]
all_header = []
all_header.append(col_labels)
df2 = pd.DataFrame(all_header)

frames = [df2, df1]
df3 = pd.concat(frames)

df4 = df3.rename(columns=df3.iloc[0])

#df5 = df4.drop(df4.index[0])

#df5.insert(0, "stop_event_id", all_id, True)


# way to display stop id
   # list_out = re.findall(r'\d+', x)
   # print(list_out)

result = df4.to_json(orient="records")
parsed =json.loads(result)
parsed.pop(0)
with open(fname, mode = 'w', encoding = 'utf-8') as f:
  json.dump(parsed, f, indent=4)



stop.py                        
              
