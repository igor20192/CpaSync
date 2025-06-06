from apscheduler.schedulers.background import BackgroundScheduler
import logging


class Scheduler:
    def __init__(self, main_task):
        self.scheduler = BackgroundScheduler()
        self.main_task = main_task

    def start(self):
        self.scheduler.add_job(self.main_task, "cron", hour=0, minute=0)
        logging.info("Scheduler started")
        self.scheduler.start()
