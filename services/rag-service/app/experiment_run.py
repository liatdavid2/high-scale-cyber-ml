import threading
import time
from copy import deepcopy


_STAGE_ORDER = [
    "chunking",
    "embedding",
    "retrieval",
    "generation",
    "evaluation",
    "save",
]

_STAGE_LABELS = {
    "chunking": "Chunking / Knowledge Prep",
    "embedding": "Query Embedding",
    "retrieval": "Vector Retrieval",
    "generation": "LLM Generation",
    "evaluation": "Answer Evaluation",
    "save": "Save Experiment",
}


class ExperimentRunState:
    def __init__(self):
        self._lock = threading.Lock()
        self._run_counter = 0
        self._state = self._empty_state()

    def _empty_state(self):
        return {
            "run_id": None,
            "status": "IDLE",
            "query": None,
            "top_k": None,
            "use_llm": None,
            "auto_evaluate": None,
            "started_at": None,
            "finished_at": None,
            "total_ms": None,
            "experiment_id": None,
            "evaluation": None,
            "stages": [
                {
                    "id": stage_id,
                    "label": _STAGE_LABELS[stage_id],
                    "status": "PENDING",
                    "message": "Waiting",
                    "duration_ms": None,
                }
                for stage_id in _STAGE_ORDER
            ],
        }

    def start(self, query: str, top_k: int, use_llm: bool, auto_evaluate: bool):
        with self._lock:
            self._run_counter += 1
            self._state = self._empty_state()
            self._state.update({
                "run_id": self._run_counter,
                "status": "RUNNING",
                "query": query,
                "top_k": top_k,
                "use_llm": use_llm,
                "auto_evaluate": auto_evaluate,
                "started_at": time.time(),
            })

    def update_stage(self, stage_id: str, status: str, message: str = "", duration_ms=None):
        with self._lock:
            for stage in self._state["stages"]:
                if stage["id"] == stage_id:
                    stage["status"] = status
                    stage["message"] = message
                    if duration_ms is not None:
                        stage["duration_ms"] = round(float(duration_ms), 3)
                    break

    def set_experiment_id(self, experiment_id):
        with self._lock:
            self._state["experiment_id"] = experiment_id

    def set_evaluation(self, evaluation):
        with self._lock:
            self._state["evaluation"] = evaluation

    def finish(self, total_ms: float):
        with self._lock:
            self._state["status"] = "COMPLETED"
            self._state["finished_at"] = time.time()
            self._state["total_ms"] = round(float(total_ms), 3)

    def fail(self, message: str, total_ms: float | None = None):
        with self._lock:
            self._state["status"] = "ERROR"
            self._state["finished_at"] = time.time()
            if total_ms is not None:
                self._state["total_ms"] = round(float(total_ms), 3)
            # Mark the currently-running stage as failed.
            for stage in self._state["stages"]:
                if stage["status"] == "RUNNING":
                    stage["status"] = "ERROR"
                    stage["message"] = message
                    break

    def snapshot(self):
        with self._lock:
            return deepcopy(self._state)


experiment_run = ExperimentRunState()
