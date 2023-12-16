def withoutDivision(dividend, divisor):
    count = 0
    r = 0
    while (True):
        r = dividend - divisor
        dividend = r
        count += 1
        if r < divisor:
            print("Quotient result",count)
            break


print("Please enter dividend number:")
a = int(input())
print("Please enter divisor number:")
b = int(input())
withoutDivision(a, b)
