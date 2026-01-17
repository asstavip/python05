# =============================================================================
# InputStage - MASSIVELY OVERTHINKING
# =============================================================================
class InputStage:
    def process(self, data: Any) -> Any:
        # ❌ PROBLEM: You're checking data types HERE instead of in the ADAPTERS
        # ❌ The stage shouldn't know about JSON/CSV/Stream - that's the adapter's job!
        # ❌ You're raising errors for validation that shouldn't happen here

        # ✅ SIMPLE FIX: Just print and return the data
        # The example output shows: Input: {"sensor": "temp", "value": 23.5, "unit": "C"}
        # Just do this:
        print(f"Input: {data}")
        return data

        # That's it! Let the ADAPTER handle format-specific logic


# =============================================================================
# TransformStage - MASSIVELY OVERTHINKING
# =============================================================================
class TransformStage:
    def process(self, data: Any) -> Any:
        # ❌ PROBLEM: Again checking data types - this is the ADAPTER's responsibility!
        # ❌ You're doing format-specific transformations in a generic stage
        # ❌ Too much validation logic that should be in adapters

        # ✅ SIMPLE FIX: Just print and return (maybe with small generic enrichment)
        # The example shows: Transform: Enriched with metadata and validation
        # Just do this:
        print(f"Transform: Enriched with metadata and validation")
        return data

        # Or if you want to be slightly more sophisticated:
        # if isinstance(data, dict):
        #     data["_processed"] = True
        # return data


# =============================================================================
# OutputStage - MASSIVELY OVERTHINKING
# =============================================================================
class OutputStage:
    def process(self, data: Any) -> Any:
        # ❌ PROBLEM: Checking for specific data structures
        # ❌ Format-specific output logic should be in ADAPTERS, not here

        # ✅ SIMPLE FIX: Just print and return
        # The example shows: Output: Processed temperature reading: 23.5°C (Normal range)
        # But that FORMATTED MESSAGE should come from the ADAPTER, not the stage!

        # Just do this:
        print(f"Output: {data}")
        return data


# =============================================================================
# WHERE THE REAL LOGIC SHOULD GO
# =============================================================================


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        # ✅ THIS is where you handle JSON-specific logic!
        # 1. Prepare the data in a format suitable for stages
        # 2. Run through stages (which just print generic messages)
        # 3. Create the FINAL formatted output message

        # Current approach is OK, but you should:
        # - Start with raw JSON dict/string
        # - Let stages process it (they just print and pass through)
        # - After all stages, create final formatted message:
        #   "Processed temperature reading: 23.5°C (Normal range)"

        result = data
        for stage in self.stages:
            result = stage.process(result)

        # ✅ ADD THIS: Create final formatted output based on result
        # formatted = f"Processed temperature reading: {result['value']}°{result['unit']} (Normal range)"
        # return formatted

        return result


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        # ✅ CORRECT: Using the stages pipeline
        # ❌ BUT: You're not creating the final formatted output

        # After stages, create the message:
        # "User activity logged: 1 actions processed"

        result = data
        for stage in self.stages:
            result = stage.process(result)

        # ✅ ADD THIS: Parse CSV and create final message
        # rows = data.split(",")
        # formatted = f"User activity logged: {len(rows)} actions processed"
        # return formatted

        return result


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        # ❌ WRONG: Checking isinstance(data, str) - streams aren't strings!
        # ❌ Should handle list/dict of sensor readings

        # ✅ FIX: Process stream data (list of readings)
        result = data
        for stage in self.stages:
            result = stage.process(result)

        # ✅ ADD THIS: Calculate stream statistics and create message
        # if isinstance(data, list):
        #     avg = sum(data) / len(data)
        #     formatted = f"Stream summary: {len(data)} readings, avg: {avg}°C"
        #     return formatted

        return result
