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
xxxx = f"=SUMIFS('Electric Usage'!E:E, 'Electric Usage'!B:B, \
\">=\"&A{row_num},'Electric Usage'!B:B,\"<=\"&B{row_num})"
print(xxxx)