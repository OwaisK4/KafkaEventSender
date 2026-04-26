package com.example.eventgenerator

import android.annotation.SuppressLint
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.os.BatteryManager
import android.os.Build
import android.os.IBinder
import android.provider.Settings
import androidx.core.app.NotificationCompat
import com.google.android.gms.location.FusedLocationProviderClient
import com.google.android.gms.location.LocationCallback
import com.google.android.gms.location.LocationRequest
import com.google.android.gms.location.LocationResult
import com.google.android.gms.location.LocationServices
import com.google.android.gms.location.Priority
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import java.util.UUID

class LocationForegroundService : Service() {

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private lateinit var fusedLocationClient: FusedLocationProviderClient
    private lateinit var locationCallback: LocationCallback
    private val sessionId = UUID.randomUUID().toString()

    override fun onCreate() {
        super.onCreate()
        fusedLocationClient = LocationServices.getFusedLocationProviderClient(this)
        createNotificationChannel()
        startForeground(NOTIFICATION_ID, buildNotification())
        startLocationUpdates()
    }

    @SuppressLint("MissingPermission")
    private fun startLocationUpdates() {
        val request = LocationRequest.Builder(Priority.PRIORITY_HIGH_ACCURACY, INTERVAL_MS)
            .setMinUpdateIntervalMillis(INTERVAL_MS)
            .build()

        locationCallback = object : LocationCallback() {
            override fun onLocationResult(result: LocationResult) {
                val loc = result.lastLocation ?: return
                scope.launch {
                    runCatching {
                        KafkaEventSender.send(
                            buildBaseEvent(
                                "location",
                                LocationPayload(
                                    lat      = loc.latitude,
                                    lon      = loc.longitude,
                                    accuracy = loc.accuracy,
                                    speed    = loc.speed
                                )
                            )
                        )
                    }
                }
            }
        }

        fusedLocationClient.requestLocationUpdates(request, locationCallback, mainLooper)
    }

    override fun onDestroy() {
        super.onDestroy()
        fusedLocationClient.removeLocationUpdates(locationCallback)
        scope.cancel()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun buildBaseEvent(eventType: String, payload: Any) = BaseEvent(
        eventType = eventType,
        device = DeviceInfo(
            deviceId     = Settings.Secure.getString(contentResolver, Settings.Secure.ANDROID_ID),
            manufacturer = Build.MANUFACTURER,
            model        = Build.MODEL,
            osVersion    = "Android ${Build.VERSION.RELEASE}"
        ),
        session = SessionInfo(sessionId = sessionId),
        context = run {
            val cm   = getSystemService(CONNECTIVITY_SERVICE) as ConnectivityManager
            val caps = cm.getNetworkCapabilities(cm.activeNetwork)
            val net  = when {
                caps?.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)     == true -> "wifi"
                caps?.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) == true -> "cellular"
                else -> "unknown"
            }
            val bm = getSystemService(BATTERY_SERVICE) as BatteryManager
            ContextInfo(network = net, battery = bm.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY))
        },
        payload = payload
    )

    private fun createNotificationChannel() {
        val channel = NotificationChannel(
            CHANNEL_ID, "Location Tracking", NotificationManager.IMPORTANCE_LOW
        )
        getSystemService(NotificationManager::class.java).createNotificationChannel(channel)
    }

    private fun buildNotification(): Notification =
        NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Event Generator")
            .setContentText("Sending location events every 5 s")
            .setSmallIcon(android.R.drawable.ic_menu_mylocation)
            .setOngoing(true)
            .build()

    companion object {
        private const val CHANNEL_ID       = "location_tracking"
        private const val NOTIFICATION_ID  = 1
        private const val INTERVAL_MS      = 5_000L
    }
}
