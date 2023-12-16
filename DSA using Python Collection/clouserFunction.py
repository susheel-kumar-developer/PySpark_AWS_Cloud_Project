"""
Clouser function -> basically nested function should be there.
1. Clouser is a technique by which data gets attached to the code.
2. Clouser is function object that remembers values in the encoding scope even if they are not present in the memory.

"""
def outer():
    def inner():
        # Clouser is a technique by which data gets attached to the code.
        x = 200
        return x
    # Clouser is function object that remembers values in the encoding scope even if they are not present in the memory.
    return inner
returnVal = outer()
print(returnVal())