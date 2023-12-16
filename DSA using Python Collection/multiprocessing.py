import multiprocessing
def square_number():
    for i in range(5):
        print(f"Executing {i}:{i * i}")


if __name__ == "__main__":
    process1 = multiprocessing.Process(target=square_number())
    process2 = multiprocessing.Process(target=square_number())

    process1.start()
    process2.start()

    process1.join()
    process2.join()

    print("Both Processing are done executing")