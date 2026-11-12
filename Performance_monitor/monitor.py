from datetime import datetime, timezone

class PerformanceMonitor:
    def __init__(self):
        self.total_received = 0
        self.total_processed = 0
        self.total_failed = 0
        self.total_processing_time = 0.0
        self.total_valid = 0
        self.total_invalid = 0
        self.latencies = []

    def record_received(self, count):
        self.total_received += count

    def record_processed(self, count, processing_time):
        self.total_processed += count
        self.total_processing_time += processing_time

    def record_latency(self, produced_at):
        if not produced_at:
            return

        event_time = datetime.fromisoformat(produced_at.replace("Z", "+00:00"))
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        latency = (datetime.now(timezone.utc) - event_time.astimezone(timezone.utc)).total_seconds()
        self.latencies.append(latency)

    def record_failed(self, count):
        self.total_failed += count

    def record_accuracy(self, valid_count, invalid_count):

        self.total_failed += invalid_count 
        self.total_invalid += invalid_count

    def display_metrics(self):
        print("\n------------------PERFORMANCE METRICS------------------")
        print("Total received:", self.total_received)
        print("Total processed:", self.total_processed)
        print("Total failed:", self.total_failed)

        if self.total_processing_time > 0:
            throughput = self.total_processed / self.total_processing_time
            print("Throughput:", round(throughput, 2), "records/sec")

        if self.latencies:
            average_latency = sum(self.latencies) / len(self.latencies)
            print("Average latency:", round(average_latency * 1000, 2), "ms")

        if self.total_received > 0:
            success_rate = (self.total_processed/self.total_received)*100

            failure_rate = (self.total_failed/self.total_received)*100

            print("SUCCESS RATE:", round(success_rate,2),"%")
            print("FAILURE RATE:", round(failure_rate,2),"%")
            print("Total Failed:",self.total_failed)    

        total_checked = self.total_valid + self.total_invalid

        if total_checked > 0:

            accuracy = (
             self.total_valid / total_checked
            ) * 100

            print("Valid records:", self.total_valid)
            print("Invalid records:", self.total_invalid)

            print(
                "Data accuracy:",
                round(accuracy, 2),
                "%"
             )