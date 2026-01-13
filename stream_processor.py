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
    def __init__(self):
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
                if type(elem) is not int:
                    print("Validation: Numeric data not verified")
                    return False
        except TypeError:
            print("Validation: Numeric data not verified")
            return False
        else:
            print("Validation: Numeric data verified")
            return True


class TextProcessor(DataProcessor):
    def __init__(self):
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
        if type(data) is str:
            print("Validation: Text data verified")
            return True
        else:
            print("Validation: Text data not verified")
            return False


class LogProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        print("Initializing Log Processor...")

    def process(self, data: Any) -> str:
        print("Processing data:", data)
        if self.validate(data):
            string = str(data).split(":")[-1]
            return f"[ALERT] ERROR level detected:{string}"
        else:
            return "Cannot Proccessing Data"

    def validate(self, data: Any) -> bool:
        if type(data) is str:
            print("Validation: Log entry verified")
            return True
        else:
            print("Validation: Log entry not verified")
            return False


print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
processor = NumericProcessor()
data = [1, 2, 3, 4, 5]
print(processor.format_output(processor.process(data)), "\n")

processor = TextProcessor()
data = "Hello Nexus World"
print(processor.format_output(processor.process(data)), "\n")

processor = LogProcessor()
data = "Hello Nexus World"
print(processor.format_output(processor.process(data)), "\n")

print("=== Polymorphic Processing Demo ===")
print("Processing multiple data types through same interface...")

p = NumericProcessor()
data = [1, 2, 3, 4, 5]
print(p.format_output(p.process(data)), "\n")