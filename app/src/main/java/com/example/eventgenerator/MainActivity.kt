package com.example.eventgenerator

import android.Manifest
import android.annotation.SuppressLint
import android.location.Location
import android.content.pm.PackageManager
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.os.BatteryManager
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.core.content.ContextCompat
import androidx.lifecycle.lifecycleScope
import com.example.eventgenerator.ui.theme.EventGeneratorTheme
import com.google.android.gms.location.LocationServices
import com.google.android.gms.location.FusedLocationProviderClient
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.UUID

class MainActivity : ComponentActivity() {

    private val sessionId = UUID.randomUUID().toString()
    private lateinit var fusedLocationClient: FusedLocationProviderClient

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        fusedLocationClient = LocationServices.getFusedLocationProviderClient(this)
        enableEdgeToEdge()
        setContent {
            EventGeneratorTheme {
                var status  by remember { mutableStateOf("Press a button to send a Kafka event.") }
                var loading by remember { mutableStateOf(false) }

                // Permission launcher: on grant, immediately send the location event
                val locationPermissionLauncher = rememberLauncherForActivityResult(
                    ActivityResultContracts.RequestPermission()
                ) { granted ->
                    if (granted) {
                        loading = true
                        lifecycleScope.launch {
                            status = try {
                                fetchAndSendLocation()
                            } catch (e: Exception) {
                                "Error: ${e.message}"
                            }
                            loading = false
                        }
                    } else {
                        status = "Location permission denied"
                    }
                }

                Scaffold(modifier = Modifier.fillMaxSize()) { innerPadding ->
                    Column(
                        modifier = Modifier
                            .fillMaxSize()
                            .padding(innerPadding)
                            .padding(24.dp),
                        verticalArrangement = Arrangement.Center,
                        horizontalAlignment = Alignment.CenterHorizontally
                    ) {
                        Text(text = status, style = MaterialTheme.typography.bodyLarge)
                        Spacer(modifier = Modifier.height(24.dp))

                        // ── Button: button_pressed event ────────────────────
                        Button(
                            onClick = {
                                loading = true
                                lifecycleScope.launch {
                                    status = try {
                                        KafkaEventSender.send(
                                            buildBaseEvent("button_pressed", emptyMap<String, Any>())
                                        )
                                    } catch (e: Exception) {
                                        "Error: ${e.message}"
                                    }
                                    loading = false
                                }
                            },
                            enabled = !loading
                        ) {
                            if (loading) {
                                CircularProgressIndicator(
                                    modifier = Modifier.size(20.dp),
                                    strokeWidth = 2.dp,
                                    color = MaterialTheme.colorScheme.onPrimary
                                )
                            } else {
                                Text("Send Button Event")
                            }
                        }

                        Spacer(modifier = Modifier.height(12.dp))

                        // ── Button: location event ───────────────────────────
                        Button(
                            onClick = {
                                if (ContextCompat.checkSelfPermission(
                                        this@MainActivity,
                                        Manifest.permission.ACCESS_FINE_LOCATION
                                    ) == PackageManager.PERMISSION_GRANTED
                                ) {
                                    loading = true
                                    lifecycleScope.launch {
                                        status = try {
                                            fetchAndSendLocation()
                                        } catch (e: Exception) {
                                            "Error: ${e.message}"
                                        }
                                        loading = false
                                    }
                                } else {
                                    locationPermissionLauncher.launch(
                                        Manifest.permission.ACCESS_FINE_LOCATION
                                    )
                                }
                            },
                            enabled = !loading
                        ) {
                            Text("Send Location Event")
                        }
                    }
                }
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun getDeviceInfo() = DeviceInfo(
        deviceId     = Settings.Secure.getString(contentResolver, Settings.Secure.ANDROID_ID),
        manufacturer = Build.MANUFACTURER,
        model        = Build.MODEL,
        osVersion    = "Android ${Build.VERSION.RELEASE}"
    )

    private fun getContextInfo(): ContextInfo {
        val cm = getSystemService(CONNECTIVITY_SERVICE) as ConnectivityManager
        val caps = cm.getNetworkCapabilities(cm.activeNetwork)
        val network = when {
            caps?.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)     == true -> "wifi"
            caps?.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) == true -> "cellular"
            else -> "unknown"
        }
        val bm      = getSystemService(BATTERY_SERVICE) as BatteryManager
        val battery = bm.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY)
        return ContextInfo(network = network, battery = battery)
    }

    private fun buildBaseEvent(eventType: String, payload: Any) = BaseEvent(
        eventType = eventType,
        device    = getDeviceInfo(),
        session   = SessionInfo(sessionId = sessionId),
        context   = getContextInfo(),
        payload   = payload
    )

    @SuppressLint("MissingPermission")
    private suspend fun fetchAndSendLocation(): String {
        val location = suspendCancellableCoroutine<Location?> { cont ->
            fusedLocationClient.lastLocation
                .addOnSuccessListener { cont.resumeWith(Result.success(it)) }
                .addOnFailureListener { cont.resumeWith(Result.failure(it)) }
        } ?: return "Error: No location fix yet — try opening Maps first"

        return KafkaEventSender.send(
            buildBaseEvent(
                "location",
                LocationPayload(
                    lat      = location.latitude,
                    lon      = location.longitude,
                    accuracy = location.accuracy,
                    speed    = location.speed
                )
            )
        )
    }
}
