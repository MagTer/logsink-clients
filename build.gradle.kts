// Root build file — plugin versions only. Non-Gradle packages (a future js/)
// live beside android/ without Gradle caring about them.
plugins {
    id("com.android.library") version "8.7.3" apply false
    id("org.jetbrains.kotlin.android") version "2.1.0" apply false
}
