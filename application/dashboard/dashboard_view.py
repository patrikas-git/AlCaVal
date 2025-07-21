import json
import os
import time
import pandas as pd
from flask import Blueprint, jsonify, render_template
from database.database import Database

dashboard_blueprint = Blueprint(
    "dashboard",
    __name__,
    url_prefix="/dashboard",
    template_folder="templates",
    static_folder="static",
)


TRANSITIONS_FILE = os.path.join("/tmp", "workflow_transitions.json")
MAX_AGE_SECONDS = 15 * 60

@dashboard_blueprint.route("", strict_slashes=False, methods=["GET"])
def show_dashboard():
    return render_template("Dashboard.html.jinja")


@dashboard_blueprint.route("/dashboard-data", methods=["GET"])
def get_dashboard_data():
    """
    Checks the age of the data file and returns its contents if it's fresh.
    """
    try:
        file_mod_time = os.path.getmtime(TRANSITIONS_FILE)

        with open(TRANSITIONS_FILE, "r", encoding = 'UTF-8') as f:
            data = json.load(f)
        return jsonify({
            "last_updated": int(file_mod_time),
            "results": data
        })

    except FileNotFoundError:
        return jsonify({"error": "Data not available yet. Wait for the first job to run."}), 404


def process_relval_transitions(flask_app):
    with flask_app.app_context():
        start_time = time.time()
        relval_db = Database("relvals")
        relvals_collection = relval_db.collection

        query = {"workflows": {"$ne": []}}
        projection = {"_id": 0, "batch_name": 1, "workflows.name": 1, "workflows.status_history": 1}

        all_events = []
        for doc in relvals_collection.find(query, projection):
            for workflow in doc.get("workflows", []):
                for history_event in workflow.get("status_history", []):
                    all_events.append({
                            "workflow_name": workflow.get("name"),
                            "batch_name": doc.get("batch_name"),
                            "status": history_event.get("status"),
                            "time": history_event.get("time"),
                        })

        if not all_events:
            print("No statuses found in DB to process.")
            return

        df = pd.DataFrame(all_events)
        transitions = []
        for _, group in df.groupby("workflow_name"):
            group_transitions = pd.DataFrame(
                {
                    "from": group["status"],
                    "to": group["status"].shift(-1),
                    "start": group["time"],
                    "batch_name": group["batch_name"],
                    "duration_seconds": group["time"].shift(-1) - group["time"],
                }
            )

            group_transitions.dropna(inplace=True, subset=["to", "duration_seconds"])
            group_transitions["duration_seconds"] = group_transitions[
                "duration_seconds"
            ].astype(int)
            transitions.extend(group_transitions.to_dict("records"))

        with open(TRANSITIONS_FILE, "w", encoding = 'UTF-8') as f:
            json.dump(transitions, f)

        print(f"--- RelVal transition fetching finished in {time.time() - start_time} seconds ---")

        return jsonify(transitions)
