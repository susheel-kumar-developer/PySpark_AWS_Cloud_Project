"""Swap Array
Given an array of integers, write a function to sort the array in such a way that all even elements appear first, followed by all odd elements. You are not allowed to use any in-built sorting functions or extra arrays.

Example 1:
Input: arr = [1, 2, 3, 4, 5, 6, 7, 8]
Output: [8,2,6,4,5,3,7,1]
Explanation: """

dynamicArray = [1,2,3,4,5,6,7,8]
lastIndex = len(dynamicArray) - 1
for startIndex in range(len(dynamicArray)):
    if dynamicArray[lastIndex] % 2 == 0 and startIndex < lastIndex:
        temp = dynamicArray [startIndex]
        dynamicArray[startIndex] = dynamicArray[lastIndex]
        dynamicArray[lastIndex] = temp
    lastIndex = lastIndex - 1

for index in range(len(dynamicArray)):
    print(dynamicArray[index])