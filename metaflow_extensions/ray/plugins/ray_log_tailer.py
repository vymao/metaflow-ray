import os
import sys
import time
import glob
import threading
from pathlib import Path


class RayLogTailer:
    """
    Tails Ray log files and streams them to stdout.
    """

    def __init__(
        self, ray_temp_dir, poll_interval, include_patterns, session_dir_wait_timeout=30
    ):
        self.ray_temp_dir = ray_temp_dir
        self.poll_interval = poll_interval
        self.stop_event = threading.Event()
        self.thread = None
        self.file_positions = {}
        # Only capture worker stdout/stderr (actor output)
        # worker-*.out/err excludes system components like raylet.out, gcs_server.out
        self.include_patterns = include_patterns
        self.session_dir_wait_timeout = session_dir_wait_timeout

    def _find_ray_session_dir(self):
        """Find the Ray session directory using the session_latest symlink."""
        # Try the specified temp dir first
        if self.ray_temp_dir:
            session_latest = os.path.join(self.ray_temp_dir, "session_latest")
            print(f"[RAY_LOG_TAILER] Checking for session_latest at: {session_latest}")
            if os.path.exists(session_latest):
                return session_latest
            else:
                print(f"[RAY_LOG_TAILER] session_latest not found at: {session_latest}")
        
        # Fallback: Check Ray's default location
        default_ray_dir = "/tmp/ray"
        if os.path.exists(default_ray_dir):
            session_latest = os.path.join(default_ray_dir, "session_latest")
            if os.path.exists(session_latest):
                print(
                    f"[RAY_LOG_TAILER] Using Ray default temp dir at {default_ray_dir}",
                    file=sys.stderr,
                )
                return session_latest
        
        return None

    def _tail_file(self, filepath):
        """Tail a single log file and print new lines."""
        try:
            # Get current position or start from beginning
            if filepath not in self.file_positions:
                self.file_positions[filepath] = 0

            with open(filepath, "r") as f:
                f.seek(self.file_positions[filepath])
                new_lines = f.readlines()

                if new_lines:
                    # Print with prefix to identify source
                    filename = os.path.basename(filepath)
                    for line in new_lines:
                        print(f"[RAY:{filename}] {line.rstrip()}")

                # Update position
                self.file_positions[filepath] = f.tell()
        except Exception as e:
            # File might not exist yet or might be rotated
            pass

    def _tail_loop(self):
        """Main loop that tails all Ray log files."""
        print("[RAY_LOG_TAILER] Starting Ray log tailer...")
        print(f"[RAY_LOG_TAILER] Looking for Ray session in: {self.ray_temp_dir}")

        # Wait for the session directory to appear (Ray might still be initializing)
        session_dir = None
        logs_dir = None
        for attempt in range(self.session_dir_wait_timeout):
            session_dir = self._find_ray_session_dir()
            if session_dir:
                logs_dir = os.path.join(session_dir, "logs")
                if os.path.exists(logs_dir):
                    print(f"[RAY_LOG_TAILER] Found Ray logs directory at {logs_dir}")
                    break
            
            if attempt == 0:
                print(
                    "[RAY_LOG_TAILER] Waiting for Ray session directory to be created..."
                )
            elif attempt % 5 == 0:
                # Log periodically what we're checking
                print(
                    f"[RAY_LOG_TAILER] Still waiting... (checked {self.ray_temp_dir} and /tmp/ray)"
                )
            time.sleep(1)

        if not session_dir or not logs_dir or not os.path.exists(logs_dir):
            print(
                "[RAY_LOG_TAILER] Ray session directory not found after waiting. Cannot tail logs.",
                file=sys.stderr,
            )
            return

        # Tail worker output files in the loop
        while not self.stop_event.is_set():
            for pattern in self.include_patterns:
                full_pattern = os.path.join(logs_dir, pattern)
                for log_file in glob.glob(full_pattern):
                    self._tail_file(log_file)

            # Wait for poll_interval or until stop_event is set
            self.stop_event.wait(self.poll_interval)

        print("[RAY_LOG_TAILER] Stopped Ray log tailer")

    def start(self):
        """Start tailing Ray logs in a background thread."""
        if self.thread and self.thread.is_alive():
            return

        self.stop_event.clear()
        self.thread = threading.Thread(target=self._tail_loop, daemon=True)
        self.thread.start()
        print("[RAY_LOG_TAILER] Ray log tailer thread started")

    def stop(self):
        """Stop tailing Ray logs."""
        if not self.thread or not self.thread.is_alive():
            return

        self.stop_event.set()
        self.thread.join(timeout=5)
        print("[RAY_LOG_TAILER] Ray log tailer stopped")
