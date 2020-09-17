import itertools

for j in range(3):
    for i in itertools.combinations(flower_names, j + 1):
        print(i)
