import pandas as pd
path = r'C:\workspace\ps-energy-dl\app\etl\orders.csv (1).csv'
csv_file = pd.read_csv(path,
                       header='infer',
                       delimiter=',')
print(csv_file)
path = r'C:\workspace\ps-energy-dl\app\etl\pipe_file'
file = csv_file.to_csv(path,header=0,sep='|')

