from datetime import datetime, timedelta
from application.services.dashboard_service import DashboardService
from application import create_app
from core_lib.utils.global_config import Config


app = create_app()

scheduler = BackgroundScheduler()
dashboardService = DashboardService()
scheduler.add_job(
    func=lambda: dashboardService.process_relval_transitions(app),
    name="RelVals transition processing",
    next_run_time=datetime.now() + timedelta(seconds=60),
    misfire_grace_time=60,
    max_instances=1 # prevent duplicate job in dev
)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(debug=Config.get('development'), port=Config.get('port'), host=Config.get('host'))
