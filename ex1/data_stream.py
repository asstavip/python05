from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class DataStream(ABC):
    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class SensorStream(DataStream):
    def __init__(self, id: str):
        super().__init__()
        self.__id = id
        print("Initializing Sensor Stream...")
        print(f"Stream ID: {self.__id}, Type: Environmental Data")

        self.__total_obj = 0
        self.__temp_sum = 0.0
        self.__temp_count = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        self.__total_obj += len(data_batch)

        temps = [e["value"] for e in data_batch if e.get("type") == "temp"]
        if temps:
            current_sum = sum(temps)
            current_count = len(temps)

            self.__temp_sum += current_sum
            self.__temp_count += current_count

            batch_avg = current_sum / current_count
            return f"Sensor analysis: {len(data_batch)} readings processed, avg temp: {batch_avg:.1f}°C"

        return f"Sensor analysis: {len(data_batch)} readings processed, no temp data"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        filtered_data = []
        for element in data_batch:
            if element.get("type") == criteria or criteria in map(
                str, element.values()
            ):
                filtered_data.append(element)
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        avg = 0.0
        if self.__temp_count > 0:
            avg = self.__temp_sum / self.__temp_count
        return {"readings": self.__total_obj, "avg_temp": avg}


class TransactionStream(DataStream):
    def __init__(self, id: str):
        super().__init__()
        self.__id = id
        print("Initializing Transaction Stream...")
        print(f"Stream ID: {self.__id}, Type: Financial Data")
        self.__operations_count = 0
        self.__net_flow = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        for element in data_batch:
            if isinstance(element, dict):
                self.__operations_count += 1
                if element["action"] == "buy":
                    self.__net_flow -= element["amount"]
                if element["action"] == "sell":
                    self.__net_flow += element["amount"]
        sign = "+"
        if self.__net_flow < 0:
            sign = "-"
        return (
            f"Transaction analysis: {self.__operations_count} operations, "
            f"net flow: {sign}{self.__net_flow} units"
        )

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None,
        amount: Optional[float] = None,
    ) -> List[Any]:
        filtered_data = []
        for element in data_batch:
            if element.get("action") == criteria or criteria in map(str, element.values()):
                if amount is None or element["amount"] > amount:
                    filtered_data.append(element["amount"])
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "total_operations": self.__operations_count,
            "current_balance": self.__net_flow,
        }


class EventStream(DataStream):
    def __init__(self, id: str):
        super().__init__()
        self.__id = id
        print("Initializing Event Stream...")
        print(f"Stream ID: {self.__id}, Type: Log Data")
        self.__error_count = 0
        self.__total_events = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        for element in data_batch:
            self.__total_events += 1
            if isinstance(element, str) and "error" in element.lower():
                self.__error_count += 1
        return f"Event analysis: {len(data_batch)} events processed, errors found: {self.__error_count}"

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        filtered_data = []
        for element in data_batch:
            if isinstance(element, str) and criteria.lower() in element.lower():
                filtered_data.append(element)
        return filtered_data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"total_events": self.__total_events, "error_events": self.__error_count}


class StreamProcessor:
    def __init__(self):
        super().__init__()
        print("Processing mixed stream types through unified interface...\n")

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        results = []
        for i, (stream, batch) in enumerate(data_batches):
            result = stream.process_batch(batch)
            results.append(result)
        return results

    def print_summary(self, results: List[str]) -> None:
        print("Batch 1 Results:")
        for i, result in enumerate(results, 1):
            print("-" , result.split(",")[0])


def format_batch_for_display(stream_type: str, data_batch: List[Any]) -> str:
    """Generate a formatted batch display string based on stream type and data."""
    if stream_type == "sensor":
        formatted_items = []
        for item in data_batch:
            if isinstance(item, dict):
                formatted_items.append(f"{item.get('type')}:{item.get('value')}")
        return f"Processing sensor batch: [{', '.join(formatted_items)}]"

    elif stream_type == "transaction":
        formatted_items = []
        for item in data_batch:
            if isinstance(item, dict):
                formatted_items.append(f"{item.get('action')}:{item.get('amount')}")
        return f"Processing transaction batch: [{', '.join(formatted_items)}]"

    elif stream_type == "event":
        return f"Processing event batch: [{', '.join(data_batch)}]"

    return f"Processing batch: {data_batch}"


print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
# --- Stream Initializations ---
sensor_stream = SensorStream("SENSOR_001")

# --- Sample Data Batches and Processing ---
sensor_data = [
    {"type": "temp", "value": 22.5},
    {"type": "humidity", "value": 65},
    {"type": "pressure", "value": 1013},
]

print(format_batch_for_display("sensor", sensor_data))
print(sensor_stream.process_batch(sensor_data), "\n")

trans_stream = TransactionStream("TRANS_001")

trans_data = [
    {"action": "buy", "amount": 50},
    {"action": "sell", "amount": 150},
    {"action": "buy", "amount": 75},
]
print(format_batch_for_display("transaction", trans_data))
print(trans_stream.process_batch(trans_data), "\n")



event_stream = EventStream("EVENT_001")

event_data = ["user_login", "connection_error", "user_logout", "log_error"]
print(format_batch_for_display("event", event_data))
print(event_stream.process_batch(event_data))


print()
# --- Polymorphic Processing Demo ---
print("=== Polymorphic Stream Processing ===")


tasks = [
    (sensor_stream, sensor_data),
    (trans_stream, trans_data),
    (event_stream, event_data),
]

stream_processor = StreamProcessor()
results = stream_processor.process_all(tasks)
stream_processor.print_summary(results)
print()

print("Stream filtering active: High-priority data only")
filtered_events = event_stream.filter_data(event_data, criteria="error")
filtered_transactions = trans_stream.filter_data(
    trans_data, criteria="sell", amount=100
)
print(
    f"Filtered results: {len(filtered_events)} critical sensor alerts, {len(filtered_transactions)} large transaction:"
)


print("\nAll streams processed successfully. Nexus throughput optimal.")
