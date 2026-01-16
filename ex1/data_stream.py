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
        for element in list:
            if element.get("type") == criteria or criteria in map(
                str, element.values()
            ):
                filtered.append(element)
        return filtered

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
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        filtered_data = []
        for element in list:
            if element.get("action") == criteria or criteria in map(
                str, element.values()
            ):
                filtered.append(element)
        return filtered

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "total_operations": self.__operations_count,
            "current_balance": self.__net_flow,
        }


class EventStream(DataStream):
    self.
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class StreamProcessor:
    pass


# TODO: ALL Implementations are here like this :


print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

# 1. Initialize the specialized streams
# Constructor takes a unique Stream ID (string) [cite: 198]
# sensor_stream = SensorStream("SENSOR_001")
trans_stream = TransactionStream("TRANS_001")
# event_stream = EventStream("EVENT_001")

# --- Individual Testing ---

# # 2. SENSOR STREAM
# # Input: A list of values or labeled readings (Batch)
# # The logic needs to identify 'temp' to calculate the average
# sensor_data = [
#     {"type": "temp", "value": 22.5},
#     {"type": "humidity", "value": 65},
#     {"type": "pressure", "value": 1013},
# ]
# print(sensor_stream.process_batch(sensor_data))
# # # Stats might return the internal counter tracked by the object
# print(f"Stats: {sensor_stream.get_stats()}\n")


# # 3. TRANSACTION STREAM
# # Input: A list of financial operations
# # The logic needs to distinguish 'buy' vs 'sell' for net flow
trans_data = [
    {"action": "buy", "amount": 50},
    {"action": "sell", "amount": 150},
    {"action": "buy", "amount": 75},
]
print(trans_stream.process_batch(trans_data))
# print(f"Stats: {trans_stream.get_stats()}\n")


# # 4. EVENT STREAM
# # Input: A list of string events
# # The logic checks for keywords like 'error' [cite: 237]
# event_data = ["user_login", "connection_error", "user_logout"]
# print(event_stream.process_batch(event_data))
# print(f"Stats: {event_stream.get_stats()}\n")


# # --- Polymorphic Processing Demo ---
# print("=== Polymorphic Stream Processing ===")
# print("Processing mixed stream types through unified interface...")

# # The 'StreamProcessor' class (mentioned in instructions) would likely
# # manage this list internally. Here is the manual polymorphic loop:

# tasks = [
#     (sensor_stream, sensor_data),  # Stream 1 + Batch 1
#     (trans_stream, trans_data),  # Stream 2 + Batch 2
#     (event_stream, event_data),  # Stream 3 + Batch 3
# ]

# print()
# for i, (stream, batch) in enumerate(tasks, 1):
#     # The Loop doesn't know if it's Sensor, Transaction, or Event.
#     # It just calls 'process_batch' on whatever 'stream' is.
#     result = stream.process_batch(batch)
#     print(f"Batch {i} Results:")
#     print(result)

# print("\nAll streams processed successfully. Nexus throughput optimal.")
