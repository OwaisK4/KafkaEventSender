package com.example.eventgenerator

import java.util.UUID

data class DeviceInfo(
    val deviceId: String,
    val manufacturer: String,
    val model: String,
    val osVersion: String
)

data class SessionInfo(
    val sessionId: String
)

data class ContextInfo(
    val network: String,
    val battery: Int
)

data class BaseEvent(
    val eventId: String = UUID.randomUUID().toString(),
    val eventType: String,
    val timestamp: Long = System.currentTimeMillis(),
    val device: DeviceInfo,
    val session: SessionInfo,
    val context: ContextInfo,
    val payload: Any
)

// ── Payloads ────────────────────────────────────────────────────────────────

data class LocationPayload(
    val lat: Double,
    val lon: Double,
    val accuracy: Float,
    val speed: Float
)
