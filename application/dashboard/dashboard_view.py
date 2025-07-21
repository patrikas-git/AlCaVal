from flask import Blueprint, jsonify, render_template
from database.database import Database
import json
import os
import pandas as pd
import time

dashboard_blueprint = Blueprint(
    "dashboard",
    __name__,
    url_prefix="/dashboard",
    template_folder="templates",
    static_folder="static",
)


OUTPUT_FILE = os.path.join("/tmp", "workflow_transitions.json")


@dashboard_blueprint.route("", strict_slashes=False, methods=["GET"])
def show_dashboard():
    return render_template("Dashboard.html.jinja")


@dashboard_blueprint.route("/dashboard-data", methods=["GET"])
def get_dashboard_data():
    return get_histograms()


def get_histograms() -> dict:
    start_time = time.time()
    relval_db = Database("relvals")
    relvals_collection = relval_db.collection

    query = {"workflows": {"$ne": []}}
    projection = {"_id": 0, "workflows": 1}

    all_events = []
    for doc in relvals_collection.find(query, projection):
        for workflow in doc.get("workflows", []):
            workflow_name = workflow.get("name")
            for history_event in workflow.get("status_history", []):
                all_events.append(
                    {
                        "workflow_name": workflow_name,
                        "status": history_event.get("status"),
                        "time": history_event.get("time"),
                    }
                )

    if not all_events:
        print("No events found to process.")
        return

    df = pd.DataFrame(all_events)
    transitions = []
    for _, group in df.groupby("workflow_name"):
        group_transitions = pd.DataFrame(
            {
                "from": group["status"],
                "to": group["status"].shift(-1),
                "start": group["time"],
                "duration_seconds": group["time"].shift(-1) - group["time"],
            }
        )

        group_transitions.dropna(inplace=True)
        group_transitions["duration_seconds"] = group_transitions[
            "duration_seconds"
        ].astype(int)
        transitions.extend(group_transitions.to_dict("records"))

    print(f"Writing {len(transitions)} transitions to {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(transitions, f)

    print("Worker finished successfully.")
    print("--- %s seconds ---" % (time.time() - start_time))

    return jsonify(transitions)
