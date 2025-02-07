
import pandas as pd


col = ['col1','col2']
row = ['row1','row2']
data1 = [[1,2],[3,4]]
data2 = [[5,6],[7,8]]
df1 = pd.DataFrame(data=data1)
df2 = pd.DataFrame(data=data2)

print(df1)
print(df2)

df3 = df1.dot(df2)
print(df3)