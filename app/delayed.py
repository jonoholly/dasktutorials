# dask delayed API

import dask.delayed as delayed
from dask.diagnostics import ProgressBar 

PATH="/vis/"

def inc(i):
   return i + 1

def add(x, y):
   return x + y

x = delayed(inc)(1)
y = delayed(inc)(2)
z = delayed(add)(x, y)

z.visualize(filename=PATH+"simple.png") 

def add_two(x):
    return x + 2

def sum_two_numbers(x,y):
    return x + y

def multiply_four(x):
    return x * 4

data = [1, 5, 8, 10]

step1 = [delayed(add_two)(i) for i in data]
step2 = [delayed(multiply_four)(j) for j in step1]
total = delayed(sum)(step2)
total.visualize(filename=PATH+"multiplysum.png")

# add another layer
data2 = [delayed(sum_two_numbers)(k, total) for k in data]
total2 = delayed(sum)(data2)
total2.visualize(filename=PATH+"multiplysum2.png")

# persisting calculations - one node
total_p = total.persist()
data2_p = [delayed(sum_two_numbers)(l, total_p) for l in data]
total2_p = delayed(sum)(data2)
total2_p.visualize()
total2_p.visualize(filename=PATH+"multiplysum2persisted.png")