import pandas as pd

x = pd.Timestamp.now()
print(x.strftime('%b %Y'))

a = [1, 2, 3, 4, 5]
b = [6, 7, 8, 9, 0]

for x, y in zip(a, b):
    print(f"{x} - {y}")

x = 11
y = x - 1 if x - 1 > 0 else 12
print(y)

print('')

ts = pd.Timestamp(year=2024, month=1, day=1, tz='America/New_York')
print(ts)
print(ts.month)

print('')

ts2 = pd.Timestamp('12/14/2023', tz='America/New_York')
print(ts2)

row_num = 4
xxxx = f"=SUMIFS('Electric Usage'!E:E, 'Electric Usage'!B:B,\
            \">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})"
print(xxxx)

print()

data1 = [
    {'a': 49, 'b': 95, 'c': 9999, 'd':1},
    {'a': 491, 'b': 48, 'c': 434, 'd':11},
    {'a': 419, 'b': 948, 'c': 9923299, 'd':111},
    {'a': 434, 'b': 8, 'c': 99599, 'd':11}
]

data2 = [
    {'a': 424, 'b': 345, 'c': 9999, 'd':1},
    {'a': 14, 'b': 725, 'c': 434, 'd':23435},
    {'a': 434, 'b': 72345634, 'c': 345, 'd':234},
    {'a': 1, 'b': 8, 'c': 4, 'd':11}
]

farts = [55, 235, 434, 3435]

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)
print(df1.query('c in a'))