#!/usr/bin/env python3
"""
Pydantic models for event validation and API responses
"""

from datetime import datetime
from typing import Dict, Any, Optional, List, Literal
from pydantic import BaseModel, Field, validator
from enum import Enum


class EventType(str, Enum):
    """Supported event types"""
    LOGIN = "login"
    LOGOUT = "logout"
    PAGE_VIEW = "page_view"
    PURCHASE = "purchase"
    SIGNUP = "signup"
    PROFILE_UPDATE = "profile_update"
    AI_REQUEST = "ai_request"


class PurchaseProperties(BaseModel):
    """Properties for purchase events"""
    product_id: int = Field(..., gt=0, description="Product ID")
    amount: float = Field(..., gt=0, description="Purchase amount")
    currency: str = Field(default="USD", description="Currency code")
    category: str = Field(..., description="Product category")


class PageViewProperties(BaseModel):
    """Properties for page view events"""
    page_url: str = Field(..., description="Page URL")
    referrer: Optional[str] = Field(None, description="Referrer URL")
    page_title: str = Field(..., description="Page title")


class SignupProperties(BaseModel):
    """Properties for signup events"""
    email: str = Field(..., description="User email")
    name: str = Field(..., description="User name")
    age: int = Field(..., ge=13, le=120, description="User age")
    country: str = Field(..., description="User country")


class AIRequestProperties(BaseModel):
    """Properties for AI request events"""
    question: str = Field(..., min_length=1, max_length=2000, description="User's question for AI")
    context: Optional[str] = Field(None, max_length=5000, description="Additional context for the AI")
    model: str = Field(default="gpt-3.5-turbo", description="OpenAI model to use")
    max_tokens: int = Field(default=500, ge=1, le=4000, description="Maximum tokens for response")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0, description="Creativity/randomness of response")


class BaseEvent(BaseModel):
    """Base event model"""
    event_id: str = Field(..., description="Unique event identifier")
    user_id: int = Field(..., gt=0, description="User identifier")
    event_type: EventType = Field(..., description="Type of event")
    timestamp: datetime = Field(default_factory=datetime.now, description="Event timestamp")
    user_agent: Optional[str] = Field(None, description="User agent string")
    ip_address: Optional[str] = Field(None, description="User IP address")
    session_id: Optional[str] = Field(None, description="Session identifier")
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v


class EventWithProperties(BaseEvent):
    """Event with dynamic properties"""
    properties: Dict[str, Any] = Field(default_factory=dict, description="Event-specific properties")


class PurchaseEvent(BaseEvent):
    """Purchase event with validated properties"""
    event_type: Literal[EventType.PURCHASE] = EventType.PURCHASE
    properties: PurchaseProperties


class PageViewEvent(BaseEvent):
    """Page view event with validated properties"""
    event_type: Literal[EventType.PAGE_VIEW] = EventType.PAGE_VIEW
    properties: PageViewProperties


class SignupEvent(BaseEvent):
    """Signup event with validated properties"""
    event_type: Literal[EventType.SIGNUP] = EventType.SIGNUP
    properties: SignupProperties


class LoginEvent(BaseEvent):
    """Login event"""
    event_type: Literal[EventType.LOGIN] = EventType.LOGIN
    properties: Dict[str, Any] = Field(default_factory=dict)


class LogoutEvent(BaseEvent):
    """Logout event"""
    event_type: Literal[EventType.LOGOUT] = EventType.LOGOUT
    properties: Dict[str, Any] = Field(default_factory=dict)


class ProfileUpdateEvent(BaseEvent):
    """Profile update event"""
    event_type: Literal[EventType.PROFILE_UPDATE] = EventType.PROFILE_UPDATE
    properties: Dict[str, Any] = Field(default_factory=dict)


class AIRequestEvent(BaseEvent):
    """AI request event with validated properties"""
    event_type: Literal[EventType.AI_REQUEST] = EventType.AI_REQUEST
    properties: AIRequestProperties


# API Response Models

class EventResponse(BaseModel):
    """Response for single event operations"""
    success: bool
    event_id: str
    message: str


class BatchEventResponse(BaseModel):
    """Response for batch event operations"""
    success: bool
    processed_count: int
    failed_count: int
    failed_events: List[str] = Field(default_factory=list)
    message: str


class PipelineStatus(BaseModel):
    """Pipeline health status"""
    status: Literal["healthy", "degraded", "unhealthy"]
    kafka_connected: bool
    database_connected: bool
    events_processed_24h: int
    last_event_timestamp: Optional[datetime]
    consumer_lag: Optional[int]


class EventStats(BaseModel):
    """Event statistics"""
    total_events: int
    events_by_type: Dict[str, int]
    events_last_hour: int
    events_last_24h: int
    avg_events_per_minute: float


class AnalyticsSummary(BaseModel):
    """Analytics summary response"""
    event_distribution: Dict[str, Dict[str, Any]]
    hourly_distribution: List[Dict[str, Any]]
    weekend_stats: Dict[str, Dict[str, Any]]
    total_revenue: float
    total_events: int


class HealthResponse(BaseModel):
    """Health check response"""
    status: Literal["ok", "degraded", "error"]
    timestamp: datetime = Field(default_factory=datetime.now)
    version: str = "0.1.0"
    uptime_seconds: float
    checks: Dict[str, bool]


# AI-specific Response Models

class AIRequestResponse(BaseModel):
    """Response for AI request submission"""
    success: bool
    request_id: str
    message: str
    estimated_wait_time_seconds: Optional[int] = None


class AIStatusResponse(BaseModel):
    """Response for AI request status check"""
    request_id: str
    status: Literal["pending", "processing", "completed", "failed"]
    question: str
    answer: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    processing_time_seconds: Optional[float] = None


class AIRequestsListResponse(BaseModel):
    """Response for listing AI requests"""
    requests: List[AIStatusResponse]
    total_count: int
    pending_count: int
    completed_count: int


# Utility functions for event validation

def validate_event(event_data: Dict[str, Any]) -> BaseEvent:
    """Validate and return the appropriate event model"""
    event_type = event_data.get("event_type")
    
    if event_type == EventType.PURCHASE:
        return PurchaseEvent(**event_data)
    elif event_type == EventType.PAGE_VIEW:
        return PageViewEvent(**event_data)
    elif event_type == EventType.SIGNUP:
        return SignupEvent(**event_data)
    elif event_type == EventType.LOGIN:
        return LoginEvent(**event_data)
    elif event_type == EventType.LOGOUT:
        return LogoutEvent(**event_data)
    elif event_type == EventType.PROFILE_UPDATE:
        return ProfileUpdateEvent(**event_data)
    elif event_type == EventType.AI_REQUEST:
        return AIRequestEvent(**event_data)
    else:
        # Fallback to generic event
        return EventWithProperties(**event_data)


def event_to_dict(event: BaseEvent) -> Dict[str, Any]:
    """Convert event model to dictionary for Kafka serialization"""
    data = event.model_dump()
    # Convert datetime to ISO string for JSON serialization
    if isinstance(data.get('timestamp'), datetime):
        data['timestamp'] = data['timestamp'].isoformat()
    return data 