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
        self.id = id
        print("Initializing Sensor Stream...")
        print("Stream ID:", self.id, "Type: Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        string = "Processing sensor batch: ["
        for i in data_batch:
            for i,v in enumerate(i.values(),1):
                if i % 2 == 1:
                    string += f"{v} :"
                else:
                    string += f"{v},"
            # string += ","
        print(string)
    
    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class TransactionStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        pass


class EventStream(DataStream):
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
















#TODO: ALL Implementations are here like this : 


print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

# 1. Initialize the specialized streams
# Constructor takes a unique Stream ID (string) [cite: 198]
sensor_stream = SensorStream("SENSOR_001")
# trans_stream = TransactionStream("TRANS_001")
# event_stream = EventStream("EVENT_001")

# --- Individual Testing ---

# 2. SENSOR STREAM
# Input: A list of values or labeled readings (Batch)
# The logic needs to identify 'temp' to calculate the average
sensor_data = [
    {"type": "temp", "value": 22.5},
    {"type": "humidity", "value": 65},
    {"type": "pressure", "value": 1013},
]
print(sensor_stream.process_batch(sensor_data))
# # Stats might return the internal counter tracked by the object
# print(f"Stats: {sensor_stream.get_stats()}\n")


# # 3. TRANSACTION STREAM
# # Input: A list of financial operations
# # The logic needs to distinguish 'buy' vs 'sell' for net flow
# trans_data = [
#     {"action": "buy", "amount": 100},
#     {"action": "sell", "amount": 150},
#     {"action": "buy", "amount": 75},
# ]
# print(trans_stream.process_batch(trans_data))
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