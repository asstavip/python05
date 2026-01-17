from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


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
        formatted_items = []
        for item in data_batch:
            if isinstance(item, dict):
                formatted_items.append(f"{item.get('type')}: {item.get('value')}")
        string = "Processing sensor batch: ["
        for i, ele in enumerate(formatted_items):
            if i != len(formatted_items) - 1:
                string += f"{ele}, "
            else:
                string += f"{ele}"
        string += "]"
        print(string)
        self.__total_obj += len(data_batch)

        temps = [e["value"] for e in data_batch if e.get("type") == "temp"]

        if temps:
            current_sum = sum(temps)
            current_count = len(temps)

            self.__temp_sum += current_sum
            self.__temp_count += current_count

            batch_avg = current_sum / current_count
            return f"Sensor analysis: {len(data_batch)} readings processed, avg temp: {batch_avg:.2f}C"

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
            if isinstance(element,dict) :
                self.__operations_count += 1
                if element["action"] == "buy":
                    self.__net_flow -= element["amount"]
                if element["action"] == "sell":
                    self.__net_flow += element["amount"]
        return (
            f"Transaction analysis: {self.__operations_count} operations, "
            f"net flow: {self.__net_flow} units"
        )

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None,amount: Optional[float] = None
    ) -> List[Any]:
        filtered_data = []
        for element in data_batch:
            if element.get("action") == criteria or criteria in map(
                str, element.values()
            ):
                if amount is None or element['amount'] > amount:
                    filtered_data.append(element['amount'])
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
            if isinstance(element,str) and "error" in element.lower():
                self.__error_count += 1
        return f"Event analysis: {len(data_batch)} events processed, errors found: {self.__error_count}"
    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        filtered_data = []
        for element in data_batch:
            if isinstance(element,str) and criteria.lower() in element.lower():
                filtered_data.append(element)
        return filtered_data
            

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "total_events": self.__total_events,
            "error_events": self.__error_count,
        }

class StreamProcessor:
    def __init__(self):
        super().__init__()
        print("Processing mixed stream types through unified interface...")        

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        results = []
        for i, (stream, batch) in enumerate(data_batches):
            result = stream.process_batch(batch)
            results.append(result)
        return results

    def print_summary(self, results: List[str]) -> None:
        print(f"Batch 1 Results:")
        for i, result in enumerate(results, 1):
            print(result)




print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
# --- Stream Initializations ---
sensor_stream = SensorStream("SENSOR_001")

# --- Sample Data Batches and Processing ---
sensor_data = [
    {"type": "temp", "value": 22.5},
    {"type": "humidity", "value": 65},
    {"type": "pressure", "value": 1013},
]
print(sensor_stream.process_batch(sensor_data))
# # # Stats might return the internal counter tracked by the object
print(f"Stats: {sensor_stream.get_stats()}\n")

trans_stream = TransactionStream("TRANS_001")

# --- Sample Data Batches and Processing ---
# # Input: A list of financial operations
# # The logic needs to distinguish 'buy' vs 'sell' for net flow
trans_data = [
    {"action": "buy", "amount": 50},
    {"action": "sell", "amount": 150},
    {"action": "buy", "amount": 75},
]
print(trans_stream.process_batch(trans_data))
print(f"Stats: {trans_stream.get_stats()}\n")

event_stream = EventStream("EVENT_001")
# --- Sample Data Batches and Processing ---
# Input: A list of log event strings
# The logic needs to count error occurrences
event_data = ["user_login", "connection_error", "user_logout"]
print(event_stream.process_batch(event_data))
print(f"Stats: {event_stream.get_stats()}\n")


# --- Polymorphic Processing Demo ---
print("=== Polymorphic Stream Processing ===")


tasks = [
    (sensor_stream, sensor_data),  # Stream 1 + Batch 1
    (trans_stream, trans_data),  # Stream 2 + Batch 2
    (event_stream, event_data),  # Stream 3 + Batch 3
]

stream_processor = StreamProcessor()
results = stream_processor.process_all(tasks)
stream_processor.print_summary(results)


print("Stream filtering active: High-priority data only")
filtered_events = event_stream.filter_data(event_data, criteria="error")
filtered_transactions = trans_stream.filter_data(trans_data, criteria="sell", amount=100)
print(
    f"Filtered results: {len(filtered_events)} critical sensor alerts, {len(filtered_transactions)} large transaction:"
)


print("\nAll streams processed successfully. Nexus throughput optimal.")
