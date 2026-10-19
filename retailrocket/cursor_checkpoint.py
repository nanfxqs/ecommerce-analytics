"""Bounded durable ACK-prefix checkpoints; uncertain suffixes replay by identity."""
from threading import Event, RLock, Thread
import time

class CursorCheckpoint:
    def __init__(self, state, persist, records=1, seconds=1):
        if type(records) is not int or not 1 <= records <= 1000:
            raise ValueError('cursor_flush_records must be an integer from1to1000')
        if not isinstance(seconds,(int,float)) or not 0 < seconds <= 1:
            raise ValueError('cursor_flush_seconds must be positive and at most1')
        self.state=state;self.persist=persist;self.records=records;self.seconds=seconds
        self.lock=RLock();self.saved=state.get('cursor',0);self.last=time.monotonic();self.stop=Event()
        self.thread=None;self.error=None
        if records>1:
            self.thread=Thread(target=self._periodic,daemon=True);self.thread.start()
    def save(self):
        with self.lock:
            if self.error:raise RuntimeError('ACK cursor persistence failed') from self.error
            self.persist(dict(self.state));self.saved=self.state.get('cursor',0);self.last=time.monotonic()
    def acknowledged(self, **values):
        with self.lock:
            self.state.update(values)
            if self.state['cursor']-self.saved>=self.records or time.monotonic()-self.last>=self.seconds:self.save()
    def _periodic(self):
        while not self.stop.wait(min(.1,self.seconds)):
            try:
                with self.lock:
                    if self.state.get('cursor',0)!=self.saved and time.monotonic()-self.last>=self.seconds:self.save()
            except Exception as error:
                self.error=error;self.stop.set()
    def close(self):
        self.stop.set()
        if self.thread:self.thread.join()
        self.save()
