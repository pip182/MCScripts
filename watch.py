import os
import time
import math
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import csv
import threading
from copy import copy


t1 = time.perf_counter()
file_path = "processed_files.csv"
last_row = []
count = 0

# Delete file_path if it exists
if os.path.exists(file_path):
    os.remove(file_path)


# Python debounce decorator...
# stolen from: https://gist.github.com/walkermatt/2871026
def debounce(wait):
    """ Decorator that will postpone a functions
        execution until after wait seconds
        have elapsed since the last time it was invoked. """
    def decorator(fn):
        def debounced(*args, **kwargs):
            def call_it():
                fn(*args, **kwargs)
            try:
                debounced.t.cancel()
            except AttributeError:
                pass
            debounced.t = threading.Timer(wait, call_it)
            debounced.t.start()
        return debounced
    return decorator


def write_to_file(data):
    global last_row, count

    with open(file_path, "a", newline="") as file:
        if last_row == data:
            return
        count += 1
        print(', '.join([str(count)] + data))
        writer = csv.writer(file)
        writer.writerow([str(count)] + data)
        last_row = data


class MyHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        if not event.is_directory and file_path not in event.src_path:
            dirname = os.path.dirname(event.src_path)
            dirname = dirname.replace("M:\\Homestead_Library\\Work Orders\\1\\", "")
            filename = os.path.basename(event.src_path)
            time_elapsed = f"{math.trunc(time.perf_counter() - t1)}"
            out = [dirname, filename, event.event_type, time_elapsed]
            write_to_file(out)


if __name__ == "__main__":
    path = os.getcwd()
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    print(f"Watching {path} for changes...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
