# TODO

~~1. AI agent data source not polling from db:~~

The Problem: The current flow is: Producer -> Kafka -> Consumer -> Database, and then AI Agent -> Database. 

The ai_request event is sent through Kafka, but the AI Agent doesn't consume it. Instead, it relies on the Consumer to write to the database first. This creates a tight coupling between the AI Agent and the database state, and it bypasses the real-time, streaming benefits of Kafka for this part of the workflow. Polling a database is often less efficient and scalable than consuming from a Kafka topic.

A More "Kafka-native" Approach: A more idiomatic design would be:

Create a dedicated Kafka topic for AI requests (e.g., ai_requests_topic).

The Producer API would publish ai_request events directly to this new topic.

The AI Agent service would be a Kafka consumer for the ai_requests_topic.

This would eliminate the need for the AI Agent to poll the database. It would react to events in real-time as they arrive in Kafka. The database would still be used to store the state of the request, but Kafka would be the primary mechanism for distributing the work.

2. Synchronous send() in Producer: The producer's send_event method calls future.get(timeout=10). 

This makes the send operation synchronous, blocking the execution until the message is acknowledged by Kafka. 

While this ensures the message is sent, it can significantly limit the throughput of the producer API under high load. A more performant approach would be to send asynchronously and handle delivery reports (or failures) in a background callback. For the current low-traffic use case, this is acceptable, but it's not ideal for a high-performance system.