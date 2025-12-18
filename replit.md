# Telegram Media Bot - Replit Setup

## Overview
This project is a Telegram bot designed to download and forward media from private and public Telegram channels. Its primary purpose is to offer media forwarding with advanced features such as download concurrency management, a premium user system, and ad monetization integration. The bot leverages Telethon for Telegram interaction and includes a WSGI server for handling ad verification, aiming to provide a robust and monetizable media sharing solution.

## User Preferences
None specified yet. Add preferences as they are expressed.

## System Architecture

### Technology Stack
- **Language:** Python 3.12
- **Telegram Client:** Telethon
- **Web Server:** Waitress (WSGI server)
- **Database:** SQLite
- **Event Loop:** uvloop

### Core Features & Design Decisions

1.  **Media Handling:**
    *   **Concurrency Control:** Implements download management with concurrency limits (no queue, immediate rejection when busy).
    *   **Efficient Media Group Downloads:** Processes media group files sequentially (download, upload, delete) to prevent high RAM usage, uploading them as individual messages rather than grouped albums for memory efficiency.
    *   **Per-File Timeout for Media Groups (Dec 2025):** Each file in a media group now gets its own 45-minute timeout (2700 seconds) instead of sharing a single timeout for the entire group. This prevents large files from starving smaller ones and ensures each file has adequate time to complete, regardless of how many files are in the group.
    *   **Hybrid Transfer Approach (Nov 2025):**
        *   **Downloads:** Uses Telethon's native streaming (`client.iter_download()`) for single-connection, chunk-by-chunk downloads that minimize RAM usage and prevent spikes on constrained environments like Render.
        *   **Uploads:** Continues using FastTelethon with optimized parallel connections (3-6 connections based on file size) for faster upload speeds while maintaining RAM efficiency.
        *   This hybrid approach provides the best balance between speed and memory safety, preventing crashes on Render's free tier while maintaining good performance.

2.  **User & Session Management:**
    *   **Authentication & Access Control:** Features a user authentication and permission system, including phone-based authentication for restricted content.
    *   **Session Pooling:** Manages user sessions with a maximum of 5 concurrent sessions and a 10-minute idle timeout.
    *   **Smart Session Eviction:** Protects active downloads by only evicting idle sessions when all slots are busy, ensuring uninterrupted user experience.
    *   **Smart Session Timeout (Dec 2025):** Sessions with active downloads are NEVER disconnected due to idle timeout. The periodic cleanup task checks `download_manager.active_downloads` before expiring any session, ensuring downloads complete successfully. Sessions are only cleaned up after downloads finish and the idle timeout expires.
    *   **Batch Download Session Protection (Dec 2025):** Fixed critical bug where batch downloads (`/bdl` command) would fail after 10 minutes because the session was disconnected due to "idle" timeout. The fix includes:
        *   **Reference-Counted Active Downloads:** Implemented `add_active_download()` and `remove_active_download()` methods in `queue_manager.py` that use reference counting instead of simple set add/discard. This allows both batch downloads AND individual downloads within the batch to hold references - user is only removed from `active_downloads` when ALL references are released.
        *   Registers users in `active_downloads` (ref-counted) at the start of batch download
        *   Updates the session's `last_activity` timestamp for each message in the loop
        *   Uses `try/finally` to ensure cleanup when batch completes or fails
        *   This prevents the session manager from disconnecting "idle" sessions during long batch downloads, even when individual downloads complete within the batch
    *   **Legal Acceptance System:** Requires users to accept Terms & Conditions and a Privacy Policy (compliant with Indian and international laws) before using bot features, with acceptance stored persistently in the database.

3.  **Monetization & Ads:**
    *   **Ad Verification System:** Provides ad verification capabilities for monetization (URL shortener integration has been retired).
    *   **Two-Step Verification:** Employs a landing page with a "Get Verification Code" button for ad verification.

4.  **System Stability & Optimization:**
    *   **Per-User Session Transfers (Dec 2025):** Since each user authenticates with their own Telegram session, no global connection pooling is needed. Each user's session can independently use full connection capacity without affecting others.
        *   **Environment Variables:**
            *   `CONNECTIONS_PER_TRANSFER` (default: 16) - Connections per download/upload
    *   **RAM Optimization:** Implemented comprehensive memory optimizations including tiered connection scaling, asynchronous background tasks (using `asyncio.create_task`), and optimized data structures for memory monitoring.
    *   **CRITICAL: Upload Connection Limiting (Nov 13, 2025):** Fixed critical memory leak that caused crashes on Render (512MB RAM) during 90MB file uploads:
        *   **Problem:** FastTelethon was spawning 18 parallel upload connections for 90MB files, causing >120MB RAM spike at upload start, exhausting available memory and crashing the bot.
        *   **Solution:** Monkeypatched `ParallelTransferrer._get_connection_count()` in `helpers/transfer.py` to enforce strict limits:
            *   Files ≥1GB: 3 connections (~30MB RAM) - Prevents OOM on huge uploads
            *   Files 50MB-1GB: 4 connections (~40MB RAM) - Safe for Render, includes 90MB case
            *   Files <50MB: 6 connections (~60MB RAM) - Faster for small files
        *   **Impact:** 90MB files now use 4 connections (~40MB RAM) instead of 18 (~120MB), preventing crashes while maintaining good upload speed.
        *   **Logging:** Connection count is logged during upload to verify the fix is active.
    *   **Tier-Based File Cleanup (Nov 2025):** Smart cleanup system that waits before deleting files to ensure proper cache/chunk clearing:
        *   **Premium Users:** 2-second wait for optimal performance
        *   **Free Users:** 5-second wait to ensure complete cache/chunk cleanup on constrained environments
        *   This prevents RAM spikes and crashes on Render by allowing Telethon and file system to fully clear internal buffers and temporary data
    *   **Cloud-Only Backup:** Simplifies backup strategy to use only GitHub for database persistence, with automatic backups every 10 minutes.
    *   **Robust Error Handling:** Includes graceful shutdown mechanisms and proper background task tracking to prevent resource leaks and errors like "Task was destroyed but it is pending!".
    *   **Memory Leak Fixes (Dec 13, 2025):** Fixed multiple memory and disk leaks:
        *   **Telethon Client Leak on Failed Logins:** Added `client.disconnect()` in exception handlers for `FloodWaitError` and generic exceptions in `phone_auth.py`
        *   **Phone Auth Cleanup Task:** Now started in `main.py` to clean up stale auth sessions (each holding ~60-70MB)
        *   **Session Manager Cleanup Task:** Now started in `main.py` to disconnect idle sessions
        *   **Periodic File Cleanup Task:** Now started in `main.py` to remove old download files
        *   **Download Manager Sweep Task:** Added periodic sweep to clean orphaned tasks and expired cooldowns
    *   **Paid Media Detection (Dec 2025):** Added handling for Telegram's paid/premium media (`MessageMediaPaidMedia`):
        *   Detects paid media before download attempt
        *   Attempts to extract actual media from extended_media container if accessible
        *   Provides clear error message when paid content cannot be downloaded
    *   **Thumbnail Generation Optimization (Dec 2025):** Increased ffmpeg timeout from 10s to 30s to reduce thumbnail generation failures for larger video files.
    *   **Accurate Upload Speed Calculation (Dec 2025):** Fixed inaccurate upload speed display that was showing average speed since transfer start instead of current speed. The fix:
        *   Tracks bytes transferred and time between each progress update
        *   Calculates speed based on bytes transferred in the last interval (real-time speed)
        *   Falls back to average speed only for the first update when no previous data exists
        *   Results in more accurate speed display that reflects actual current transfer rate
    *   **SQLite Database:** Chosen for its portability and low resource footprint.
    *   **Waitress WSGI Server:** Selected over Flask for its minimal RAM consumption.
    *   **uvloop:** Used for performance enhancement of the event loop.

### Core Modules
-   `main.py`: Main bot logic and Telethon event handlers.
-   `server_wsgi.py`: WSGI server entry point and bot orchestration.
-   `database_sqlite.py`: Manages SQLite database operations.
-   `ad_monetization.py`: Handles ad verification.
-   `access_control.py`: Manages user permissions and authentication.
-   `queue_manager.py`: Controls download concurrency (no queue system, immediate start or rejection).
-   `session_manager.py`: Manages user session lifecycles.
-   `cloud_backup.py`: Implements the GitHub-based database backup system.
-   `legal_acceptance.py`: Manages the legal terms acceptance process.
-   `admin_commands.py`: Admin commands including broadcast and targeted messaging.

### Admin Commands
-   `/broadcast <message>` - Send message to all users OR specific users
    -   All users: `/broadcast Hello everyone!`
    -   Single user: `/broadcast @123456789 Hello!`
    -   Multiple users: `/broadcast @123456789,987654321 Important notice!`
    -   Media: Reply to photo/video/document with `/broadcast [@user_ids] <optional caption>`

## External Dependencies

-   **Telegram API:** Accessed via the Telethon library using `API_ID` and `API_HASH`.
-   **BotFather:** For obtaining the `BOT_TOKEN`.
-   **GitHub:** Used for cloud-only database backups (`GITHUB_TOKEN`, `GITHUB_BACKUP_REPO`).
-   **Payment Gateways (Optional):** Supports integration with `PAYPAL_URL`, `UPI_ID`, `TELEGRAM_TON`, `CRYPTO_ADDRESS` for premium features.