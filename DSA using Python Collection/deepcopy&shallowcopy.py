import copy


# Syntax of Deep copy-> nothing is changing in original data structure
# Syntax: copy.deepcopy(x)
#
# Syntax of Shallow copy -> Data will change in original datastructure
# Syntax: copy.copy(x)

originalData = [1,2,3]

shallowCopy = copy.copy(originalData)

shallowCopy[1] = [4,5,6,7]

deepCopy = copy.deepcopy(originalData)
deepCopy[2] = 10
print("shallowCopy->",shallowCopy, "deepcopy->",deepCopy ,"originalData->", originalData)


# import copy
#
# original_list = [1, 2, [3, 4]]
# shallow_copy = copy.copy(original_list)
#
# shallow_copy[2][0] = 99
# print(original_list)  # Output: [1, 2, [99, 4]]

