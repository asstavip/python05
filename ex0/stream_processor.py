from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        print("Initializing Numeric Processor...")

    def process(self, data: Any) -> str:
        print("Processing data:", data)
        if self.validate(data) == False:
            return "Cannot Proccessing Data"
        return (
            f"Processed {len(data)} numeric values, sum={sum(data)},"
            f" avg={sum(data) / len(data)}"
        )

    def validate(self, data: Any) -> bool:
        try:
            for elem in data:
                if not isinstance(elem, int):
                    print("Validation: Numeric data not verified")
                    return False
        except TypeError:
            print("Validation: Numeric data not verified")
            return False
        else:
            print("Validation: Numeric data verified")
            return True


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        print("Initializing Text Processor...")

    def process(self, data: Any) -> str:
        print("Processing data:", data)
        if self.validate(data):
            string = str(data)
            words = string.split(" ")
            return f"Processed text: {len(string)} characters, {len(words)} words"
        else:
            return "Cannot Proccessing Data"

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            print("Validation: Text data verified")
            return True
        else:
            print("Validation: Text data not verified")
            return False


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        print("Initializing Log Processor...")

    def process(self, data: Any) -> str:
        print("Processing data:", data)
        if self.validate(data):
            string = str(data).split(":")
            message = "ERROR"
            if string[0] != "ERROR":
                message = string[0]
            return f"{[message]} {message} level detected:{string[-1]}"
        else:
            return "Cannot Proccessing Data"

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            print("Validation: Log entry verified")
            return True
        else:
            print("Validation: Log entry not verified")
            return False


print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
processor_numeric = NumericProcessor()
data = [1, 2, 3, 4, 5]
print(processor_numeric.format_output(processor_numeric.process(data)), "\n")

processor_text = TextProcessor()
data = "Hello Nexus World"
print(processor_text.format_output(processor_text.process(data)), "\n")

processor_log = LogProcessor()
data = "Hello Nexus World"
print(processor_log.format_output(processor_log.process(data)), "\n")

print("=== Polymorphic Processing Demo ===")
print("Processing multiple data types through same interface...")

tasks = [
    (processor_numeric, [1, 2, 3]),
    (processor_text, "Hello Nexus"),
    (processor_log, "INFO: System ready"),
]
print()
for i, (pr, data) in enumerate(tasks, 1):
    result = pr.format_output(pr.process(data))
    print(f"Result {i}: {result}\n")


print("Foundation systems online. Nexus ready for advanced streams.")
