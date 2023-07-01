import pandas as pd 
import os

df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
if not os.path.exists('data'):
        os.makedirs('data')
df.to_csv('data/test.csv', index=False)