-- PostgreSQL Database Initialization Script
-- This script runs automatically when the PostgreSQL container starts for the first time

-- Create database if it doesn't exist (this is handled by POSTGRES_DB environment variable)
-- The database 'kafka_pipeline' will be created automatically

-- Set timezone
SET timezone = 'UTC';

-- Create user_events table for raw event data
CREATE TABLE IF NOT EXISTS user_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE NOT NULL,
    user_id INTEGER NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    user_agent TEXT,
    ip_address INET,
    session_id VARCHAR(255),
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create transformed_events table for processed data
CREATE TABLE IF NOT EXISTS transformed_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE NOT NULL,
    user_id INTEGER NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_date DATE NOT NULL,
    event_hour INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    revenue DECIMAL(10,2) DEFAULT 0,
    country VARCHAR(100),
    category VARCHAR(100),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create ai_requests table for AI request processing
CREATE TABLE IF NOT EXISTS ai_requests (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(255) UNIQUE NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    user_id INTEGER NOT NULL,
    question TEXT NOT NULL,
    context TEXT,
    model VARCHAR(100) DEFAULT 'gpt-3.5-turbo',
    max_tokens INTEGER DEFAULT 500,
    temperature DECIMAL(3,2) DEFAULT 0.7,
    status VARCHAR(50) DEFAULT 'pending',
    answer TEXT,
    error_message TEXT,
    processing_time_seconds DECIMAL(10,3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_processing_at TIMESTAMP,
    completed_at TIMESTAMP,
    CONSTRAINT ai_requests_status_check CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_event_type ON user_events(event_type);
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_transformed_events_user_id ON transformed_events(user_id);
CREATE INDEX IF NOT EXISTS idx_transformed_events_event_date ON transformed_events(event_date);
CREATE INDEX IF NOT EXISTS idx_ai_requests_status ON ai_requests(status);
CREATE INDEX IF NOT EXISTS idx_ai_requests_user_id ON ai_requests(user_id);
CREATE INDEX IF NOT EXISTS idx_ai_requests_created_at ON ai_requests(created_at);
CREATE INDEX IF NOT EXISTS idx_ai_requests_request_id ON ai_requests(request_id);

-- Insert some sample data for testing (optional)
-- INSERT INTO user_events (event_id, user_id, event_type, timestamp, properties) VALUES
-- ('test-event-1', 1001, 'login', NOW(), '{"source": "web"}'),
-- ('test-event-2', 1002, 'page_view', NOW(), '{"page": "/home"}'),
-- ('test-event-3', 1003, 'purchase', NOW(), '{"amount": 29.99, "product": "widget"}');

-- Grant necessary permissions (the user is already the owner, but just to be explicit)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kafka_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kafka_user;

-- Print completion message
\echo 'Database initialization completed successfully!'
\echo 'Tables created: user_events, transformed_events, ai_requests'
\echo 'Indexes created for optimal performance'
\echo 'Database is ready for Kafka AI Pipeline!' 