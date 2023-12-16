#         countAdjacentPairs("dog cat", 0);                 // 0
#         countAdjacentPairs("dog dog cat", 1);             // 1
#         countAdjacentPairs("apple dog cat", 0);           // 0
#         countAdjacentPairs("pineapple apple dog cat", 0); // 0
#         countAdjacentPairs("apple     apple dog cat", 1); // 1
#         countAdjacentPairs("apple dog apple dog cat", 0); // 0
#         countAdjacentPairs("dog dog dog dog dog dog", 1); // 1
#         countAdjacentPairs("dog dog dog dog cat cat", 2); // 2
#         countAdjacentPairs("cat cat dog dog cat cat", 3); // 3
#         countAdjacentPairs("cat cat cat dog dog", 2);     // 2

text = "dog dog dog  cat cat dog dog cat cat"
list_len = []

words = text.split()
flag = 0
count = 0
for index in range(len(words)-1):
    if words[index] == words[index+1] and flag != 1 :
        count +=1
        flag = 1
    elif words[index] != words[index+1]:
        flag = 0
print(count)