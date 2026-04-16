package com.example.eventgenerator

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.concurrent.TimeUnit

// REST_PROXY_URL and TOPIC are injected from local.properties via BuildConfig
private val REST_PROXY_URL = BuildConfig.REST_PROXY_URL
private val TOPIC          = BuildConfig.TOPIC

// Confluent REST Proxy request/response shapes
private data class ProduceRecord(val key: String, val value: BaseEvent)
private data class ProduceRequest(val records: List<ProduceRecord>)
private data class OffsetResult(val partition: Int, val offset: Long, val error: String?)
private data class ProduceResponse(val offsets: List<OffsetResult>)

private interface KafkaRestProxy {
    // Content-Type header is added via the OkHttp interceptor below
    @POST("topics/{topic}")
    suspend fun produce(
        @Path("topic") topic: String,
        @Body request: ProduceRequest
    ): ProduceResponse
}

private val retrofit = Retrofit.Builder()
    .baseUrl(REST_PROXY_URL)
    .client(
        OkHttpClient.Builder()
            .connectionPool(okhttp3.ConnectionPool(0, 1, TimeUnit.MILLISECONDS))
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .addNetworkInterceptor { chain ->
                // Confluent REST Proxy requires this Content-Type for raw JSON records
                val request = chain.request().newBuilder()
                    .header("Content-Type", "application/vnd.kafka.json.v2+json")
                    .header("Accept",       "application/vnd.kafka.v2+json")
                    .build()
                chain.proceed(request)
            }
            .build()
    )
    .addConverterFactory(GsonConverterFactory.create())
    .build()

private val proxy = retrofit.create(KafkaRestProxy::class.java)

object KafkaEventSender {
    suspend fun send(event: BaseEvent): String = withContext(Dispatchers.IO) {
        val request  = ProduceRequest(records = listOf(ProduceRecord(key = event.eventId, value = event)))
        val response = proxy.produce(TOPIC, request)
        val first    = response.offsets.first()
        if (first.error != null) throw Exception(first.error)
        "Sent → partition=${first.partition} offset=${first.offset}"
    }
}
