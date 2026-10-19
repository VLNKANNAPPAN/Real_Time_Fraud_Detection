"""Module entry point that works without refreshing an existing editable install."""

from fraud_engine.kafka_io import processor_main

if __name__ == "__main__":
    processor_main()
