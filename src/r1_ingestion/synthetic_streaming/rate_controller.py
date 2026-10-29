"""Precise monotonic rate controller for pacing synthetic stream emission."""

from __future__ import annotations

import threading
import time


class RateController:
    """Controls event generation pacing using a monotonic clock to sustain target TPS without drift or bursts."""

    def __init__(self, target_tps: float = 10.0):
        self._lock = threading.Lock()
        self._target_tps = max(0.1, float(target_tps))
        self._running = False
        self._paused = False

        # Non-destructive actual TPS tracking
        now = time.monotonic()
        self._window_start_time = now
        self._window_event_count = 0
        self._current_actual_tps = 0.0

        # Pacing state
        self._next_event_time = 0.0

    @property
    def target_tps(self) -> float:
        with self._lock:
            return self._target_tps

    def set_target_tps(self, new_tps: float) -> None:
        """Update target TPS dynamically at runtime."""
        with self._lock:
            self._target_tps = max(0.1, float(new_tps))
            # Reschedule next slot to take effect immediately
            now = time.monotonic()
            interval = 1.0 / self._target_tps
            if self._next_event_time > now + interval:
                self._next_event_time = now + interval

    @property
    def actual_tps(self) -> float:
        """Read-only actual measured generation rate per second."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._window_start_time
            # If idle for > 2 seconds without events, report 0.0
            if elapsed >= 2.0 and self._window_event_count == 0:
                self._current_actual_tps = 0.0
            elif elapsed >= 1.0 and self._window_event_count > 0:
                # Update current estimate if window elapsed without wait_for_slot firing
                self._current_actual_tps = self._window_event_count / elapsed
                self._window_start_time = now
                self._window_event_count = 0
            return round(self._current_actual_tps, 2)

    def start(self) -> None:
        with self._lock:
            self._running = True
            self._paused = False
            now = time.monotonic()
            self._window_start_time = now
            self._window_event_count = 0
            self._current_actual_tps = 0.0
            self._next_event_time = now

    def stop(self) -> None:
        with self._lock:
            self._running = False

    def pause(self) -> None:
        with self._lock:
            self._paused = True

    def resume(self) -> None:
        with self._lock:
            self._paused = False
            self._next_event_time = time.monotonic()

    def wait_for_slot(self) -> bool:
        """Block until the next event timeslot is reached according to target TPS.
        
        Returns:
            True if slot was granted and generator is running, False if stopped.
        """
        while True:
            with self._lock:
                if not self._running:
                    return False
                if self._paused:
                    sleep_time = 0.05
                else:
                    now = time.monotonic()
                    interval = 1.0 / self._target_tps

                    if self._next_event_time <= now:
                        # Prevent catch-up bursts: if behind by more than 1 interval, reset next slot
                        if now - self._next_event_time > interval:
                            self._next_event_time = now + interval
                        else:
                            self._next_event_time += interval

                        self._window_event_count += 1
                        elapsed = now - self._window_start_time
                        if elapsed >= 1.0:
                            self._current_actual_tps = self._window_event_count / elapsed
                            self._window_start_time = now
                            self._window_event_count = 0

                        return True

                    sleep_time = self._next_event_time - now

            if sleep_time > 0:
                # Sleep in short increments to remain responsive to stop/pause
                time.sleep(min(sleep_time, 0.02))
