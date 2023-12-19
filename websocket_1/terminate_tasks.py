import psutil
import signal

def terminate_celery_tasks():
    for proc in psutil.process_iter():
        try:
            if proc.name() == "celery":
                proc.send_signal(signal.SIGTERM)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

if __name__ == "__main__":
    terminate_celery_tasks()