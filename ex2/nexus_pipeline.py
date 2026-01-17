from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol


# the PipelineStage protocol (interface)
class PipelineStage(Protocol):
    def process(self, data: Any) -> Any:
        pass


# the ProcessingPipeline abstract base class
class ProcessingPipeline(ABC):
    def __init__(self):
        self.stages: List[PipelineStage] = []

    def add_stage(self, stage: PipelineStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


# concrete pipeline stages


class InputStage:
    def process(self, data: Any) -> Any:
        if data["type"] == "json":
            if isinstance(data, dict):
                for key, value in data.items():
                    if key != "sensor":
                        raise ValueError("Invalid JSON data")        
                return data
            print(f"Input JSON data: {data}")
        elif data["type"] == "csv":
            if not isinstance(data, str) or "," not in data:
                raise ValueError("Invalid CSV data")
            if data["value"].count(",") != 2:
                raise ValueError("Invalid CSV data")
            return data
            print(f"Input CSV data: {data}")
        elif data["type"] == "stream":
            if not isinstance(data["value"], dict[str, Any]):
                raise ValueError("Invalid Stream data")
            return data
            print(f"Input Stream data: {data}")
        else:
            raise ValueError("Unsupported data type")


class TransformStage:
    def process(self, data: Any) -> Any:
        if data["type"] == "json":
            print("Transform: Enriched with metadata and validation")
            for k,v in data.items():
                if k != "sensor":
                    raise ValueError("Invalid JSON data")                    
            data["metadata"] = {"processed_by": "JSONAdapter", "status": "validated"}
            return data
        elif data["type"] == "csv":
            print("Transform: Parsed CSV and normalized fields")
            rows = data["value"].split(",")
            if len(rows) != 3:
                raise ValueError("Invalid CSV data")
            normalized_data = {
                "field1": rows[0].strip().lower(),
                "field2": rows[1].strip().lower(),
                "field3": rows[2].strip().lower(),
            }
            return normalized_data
        elif data["type"] == "stream":
            print("Transform: Aggregated stream data and computed metrics")
            stream_data = data["value"]
            if not isinstance(stream_data, dict):
                raise ValueError("Invalid Stream data")
            aggregated_data = {
                "summary": f"Stream {stream_data.get('id')} processed",
                "metrics": {"count": len(stream_data)}
            }
            return aggregated_data

"""
Processing CSV data through same pipeline...
Input: "user,action,timestamp"
Transform: Parsed and structured data
Output: User activity logged: 1 actions processed
"""
class OutputStage:
    def process(self, data: Any) -> Any:
        if data.get("type") == "json" or "metadata" in data:
            print(
                f"Output: Processed temperature reading: {data.get('value')}°{data.get('unit')} (Normal range)"
            )
            return "JSON data processed and stored."
        if data.get("type") == "csv":
            print(f"Output: {data.get('field1')} activity logged: 1 actions processed")
            return "CSV data processed and stored."
        if data.get("type") == "stream" or "summary" in data:
            print(f"Output: {data.get('summary')} with {data.get('metrics', {}).get('count', 0)} data points")
            return "Stream data processed and stored."
# adapter classes for different data formats
# it need to add stage to the stages list
# by calling the add_stage method from the ProcessingPipeline base class
class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.__id = pipeline_id
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Any:
        if isinstance(data, dict):
            input_data = data
            for stage in self.stages:
                input_data = stage.process(input_data)
            return input_data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.__id = pipeline_id
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Any:
        if isinstance(data, str):
            input_data = data
            for stage in self.stages:
                input_data = stage.process(input_data)
            return input_data
        



class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.__id = pipeline_id
        self.add_stage(InputStage())
        self.add_stage(TransformStage())
        self.add_stage(OutputStage())

    def process(self, data: Any) -> Any:
        if isinstance(data, str):
            input_data = data
            for stage in self.stages:
                input_data = stage.process(input_data)
            return input_data


# the NexusManager class to orchestrate pipelines
class NexusManager:
    pass


