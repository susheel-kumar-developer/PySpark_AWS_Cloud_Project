# import pygame
# import time
#
# def playSound():
#     pygame.mixer.init()
#     pygame.mixer.music.load("C:\\Users\\skmjm\\Downloads\\ganpati")
#     pygame.mixer.music.play()
#
#
# def main():
#     playSound()
#
#
# if "__name__" == "__main__":
#     main()
#
#

import time
import pygame

def set_alarm():
    alarm_time = input("Enter the alarm time in seconds: ")
    return int(alarm_time)

def play_alarm_sound():
    pygame.mixer.init()
    pygame.mixer.music.load("ganpati.mp3")  # Replace with the path to your sound file
    pygame.mixer.music.play(-1)
    pygame.display.set_mode((200,200))
def main():
    alarm_time = set_alarm()

    print(f"Alarm set for {alarm_time} seconds from now.")
    time.sleep(alarm_time)

    print("Time's up! Alarm is ringing.")
    play_alarm_sound()

if __name__ == "__main__":
    # main()
    play_alarm_sound()
