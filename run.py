from datetime import datetime, timedelta
from application import create_app
from core_lib.utils.global_config import Config
from application.dashboard.dashboard_view import process_relval_transitions
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

app = create_app()

scheduler = BackgroundScheduler()
scheduler.add_job(
    func=process_relval_transitions, 
    trigger="interval", 
    minutes=15, 
    args=[app],
    id="worker_job",
    name="RelVals transition processing",
    next_run_time=datetime.now() + timedelta(seconds=60),
    misfire_grace_time=60,
    max_instances=1 # prevent duplicate job in dev
)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(debug=Config.get('development'), port=Config.get('port'), host=Config.get('host'))
