plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "se.falle.logsink"
    compileSdk = 35

    defaultConfig {
        // 26 keeps Android Automotive head units in range; raise deliberately.
        minSdk = 26
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
}

dependencies {
    api("com.jakewharton.timber:timber:5.0.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")

    // JVM unit tests (the first ones). org.json because android.jar's JSONObject is a stub
    // that throws on the JVM; mockwebserver is the sink's stand-in.
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.json:json:20240303")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}
