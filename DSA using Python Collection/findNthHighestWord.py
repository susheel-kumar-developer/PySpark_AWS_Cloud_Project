from collections import defaultdict

text = "I am a python good  programmer"
list_len = []

words = text.split()

d = defaultdict(list)

for word in words:
    d[len(word)].append(word)

sortWords = sorted(d.items())
# find third largest word in the string using -3 from end.
print(sortWords[-3])
