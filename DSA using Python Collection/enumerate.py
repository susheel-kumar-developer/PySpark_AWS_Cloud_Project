enumerateList = ["name","age","phone"]

# print(list(enumerate(enumerateList)))

for pos,ele in enumerate(enumerateList,1):
    print(f"{pos}:{ele}")


