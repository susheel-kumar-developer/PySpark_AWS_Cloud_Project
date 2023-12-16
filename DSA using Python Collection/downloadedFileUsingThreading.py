import threading
import time


def downloadFile(file):
    print(f"Downloading files{file}...")
    time.sleep(2)
    print(f"Downloaded files{file}...")


threads = []
files = ['file1.txt', 'file2.csv', 'file3.json']

for file in files:
    thread = threading.Thread(target=downloadFile, args=(file,))
    threads.append(thread)

# start the thread
for thread in threads:
    thread.start()

# wait for all the thread
for thread in threads:
    thread.join()

print("All files downloaded successfyully")