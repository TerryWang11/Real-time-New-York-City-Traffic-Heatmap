from threading import Event, Thread


def call_repeatedly(interval, func, args):
    stopped = Event()

    def loop():
        while not stopped.wait(interval):  # the first call is in `interval` secs
            return func(*args)
    Thread(target=loop).start()
    return stopped.set
