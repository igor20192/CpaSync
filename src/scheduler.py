from apscheduler.schedulers.background import BackgroundScheduler
import logging
from typing import Callable


class Scheduler:
    """
    A wrapper around APScheduler's BackgroundScheduler to schedule a daily task.

    Attributes:
        main_task (Callable): The task function to be executed daily.
        hour (int): Hour at which the task should run (0-23).
        minute (int): Minute at which the task should run (0-59).
    """

    def __init__(self, main_task: Callable, hour: int = 0, minute: int = 0):
        """
        Initialize the Scheduler.

        Args:
            main_task (Callable): The task to be scheduled.
            hour (int, optional): Hour of the day when the task should run. Defaults to 0.
            minute (int, optional): Minute of the hour when the task should run. Defaults to 0.
        """
        self.scheduler = BackgroundScheduler()
        self.main_task = main_task
        self.hour = hour
        self.minute = minute

    def start(self):
        """
        Start the scheduler and schedule the main task as a daily cron job.
        """
        try:
            self.scheduler.add_job(
                self.main_task,
                trigger="cron",
                hour=self.hour,
                minute=self.minute,
                id="daily_main_task",
                replace_existing=True,
            )
            self.scheduler.start()
            logging.info(
                f"Scheduler started: daily task scheduled at {self.hour:02d}:{self.minute:02d}"
            )
        except Exception as e:
            logging.error(f"Failed to start scheduler: {str(e)}", exc_info=True)
