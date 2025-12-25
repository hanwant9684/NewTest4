import secrets
import aiohttp
from datetime import datetime, timedelta
import time

from logger import LOGGER
from database_sqlite import db

PREMIUM_DOWNLOADS = 5
SESSION_VALIDITY_MINUTES = 30
RICHADS_PUBLISHER = "989337"
RICHADS_WIDGET = "381546"
RICHADS_API_URL = "http://15068.xml.adx1.com/telegram-mb"
RICHADS_AD_COOLDOWN = 300  # 5 minutes

class AdMonetization:
    def __init__(self):
        # All ads are on website only - no URL shorteners needed
        self.adsterra_smartlink = "https://www.effectivegatecpm.com/zn01rc1vt?key=78d0724d73f6154a582464c95c28210d"
        self.blog_url = "https://socialhub00.blogspot.com/"
        
        LOGGER(__name__).info("Ad Monetization initialized - using Adsterra SmartLink to blog")
    
    def create_ad_session(self, user_id: int) -> str:
        """Create a temporary session for ad watching"""
        session_id = secrets.token_hex(16)
        db.create_ad_session(session_id, user_id)
        
        LOGGER(__name__).info(f"Created ad session {session_id} for user {user_id}")
        return session_id
    
    def verify_ad_completion(self, session_id: str) -> tuple[bool, str, str]:
        """Verify that user clicked through URL shortener and generate verification code"""
        session_data = db.get_ad_session(session_id)
        
        if not session_data:
            return False, "", "❌ Invalid or expired session. Please start over with /getpremium"
        
        # Check if session expired (30 minutes max)
        elapsed_time = datetime.now() - session_data['created_at']
        if elapsed_time > timedelta(minutes=SESSION_VALIDITY_MINUTES):
            db.delete_ad_session(session_id)
            return False, "", "⏰ Session expired. Please start over with /getpremium"
        
        # Atomically mark session as used (prevents race condition)
        success = db.mark_ad_session_used(session_id)
        if not success:
            return False, "", "❌ This session has already been used. Please use /getpremium to get a new link."
        
        # Generate verification code
        verification_code = self._generate_verification_code(session_data['user_id'])
        
        # Delete session after successful verification
        db.delete_ad_session(session_id)
        
        LOGGER(__name__).info(f"User {session_data['user_id']} completed ad session {session_id}, generated code {verification_code}")
        return True, verification_code, "✅ Ad completed! Here's your verification code"
    
    def _generate_verification_code(self, user_id: int) -> str:
        """Generate verification code after ad is watched"""
        code = secrets.token_hex(4).upper()
        db.create_verification_code(code, user_id)
        
        LOGGER(__name__).info(f"Generated verification code {code} for user {user_id}")
        return code
    
    def verify_code(self, code: str, user_id: int) -> tuple[bool, str]:
        """Verify user's code and grant free downloads"""
        code = code.upper().strip()
        
        verification_data = db.get_verification_code(code)
        
        if not verification_data:
            return False, "❌ **Invalid verification code.**\n\nPlease make sure you entered the code correctly or get a new one with `/getpremium`"
        
        if verification_data['user_id'] != user_id:
            return False, "❌ **This verification code belongs to another user.**"
        
        created_at = verification_data['created_at']
        if datetime.now() - created_at > timedelta(minutes=30):
            db.delete_verification_code(code)
            return False, "⏰ **Verification code has expired.**\n\nCodes expire after 30 minutes. Please get a new one with `/getpremium`"
        
        db.delete_verification_code(code)
        
        # Grant ad downloads
        db.add_ad_downloads(user_id, PREMIUM_DOWNLOADS)
        
        LOGGER(__name__).info(f"User {user_id} successfully verified code {code}, granted {PREMIUM_DOWNLOADS} ad downloads")
        return True, f"✅ **Verification successful!**\n\nYou now have **{PREMIUM_DOWNLOADS} free download(s)**!"
    
    def generate_ad_link(self, user_id: int, bot_domain: str | None = None) -> tuple[str, str]:
        """
        Generate ad link - sends user to blog homepage with session
        Blog's JavaScript will automatically redirect to first verification page
        This way you can change verification pages in theme without updating bot code
        """
        session_id = self.create_ad_session(user_id)
        
        # Send to blog homepage - theme will handle redirect to first page
        first_page_url = f"{self.blog_url}?session={session_id}"
        
        # Add app_url parameter if bot domain is available
        if bot_domain:
            from urllib.parse import quote
            first_page_url += f"&app_url={quote(bot_domain)}"
        
        LOGGER(__name__).info(f"User {user_id}: Sending to blog homepage for ad verification - app_url: {bot_domain}")
        
        return session_id, first_page_url
    
    def get_premium_downloads(self) -> int:
        """Get number of downloads given for watching ads"""
        return PREMIUM_DOWNLOADS


class RichAdsMonetization:
    """RichAds integration for Telethon bot with impression tracking"""
    
    def __init__(self):
        self.user_last_ad = {}
        self.impression_count = 0
        self._last_cleanup = time.time()
        LOGGER(__name__).info("RichAds Monetization initialized")

    def _cleanup_old_cooldowns(self):
        """Remove cooldown entries older than 1 hour to prevent memory leak"""
        current_time = time.time()
        if current_time - self._last_cleanup > 3600:
            old_count = len(self.user_last_ad)
            self.user_last_ad = {k: v for k, v in self.user_last_ad.items() if current_time - v < 3600}
            cleaned = old_count - len(self.user_last_ad)
            if cleaned > 0:
                LOGGER(__name__).info(f"Cleaned {cleaned} old ad cooldown entries")
            self._last_cleanup = current_time
    
    async def show_ad(self, client, chat_id: int, user_id: int, lang_code: str = "en"):
        """Fetch and display RichAds to user with proper impression tracking"""
        try:
            # Cleanup old cooldown entries periodically
            self._cleanup_old_cooldowns()
            
            user_type = db.get_user_type(user_id)
            if user_type in ['paid', 'premium', 'admin']:
                return
            
            # Check cooldown (5 minutes per user)
            current_time = time.time()
            if user_id in self.user_last_ad:
                if current_time - self.user_last_ad[user_id] < RICHADS_AD_COOLDOWN:
                    return
            
            # Add Content-Type header as per best practices
            headers = {"Content-Type": "application/json"}
            
            async with aiohttp.ClientSession() as session:
                payload = {
                    "language_code": lang_code,
                    "publisher_id": RICHADS_PUBLISHER,
                    "widget_id": RICHADS_WIDGET,
                    "bid_floor": 0.0001,
                    "telegram_id": str(user_id),
                    "production": True
                }
                
                async with session.post(RICHADS_API_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    # Log non-200 responses for debugging
                    if resp.status != 200:
                        LOGGER(__name__).warning(f"RichAds API returned status {resp.status}")
                        return
                    
                    data = await resp.json()
                    if not isinstance(data, list) or len(data) == 0:
                        LOGGER(__name__).debug(f"RichAds returned empty response for user {user_id}")
                        return
                    
                    ad = data[0]
                    
                    # Extract ad data with fallbacks
                    caption = ad.get('message') or ad.get('title', '')
                    # IMPORTANT: 'image' URL contains impression tracking pixel
                    # 'image_preload' is direct image without tracking - use as fallback only
                    photo_url = ad.get('image') or ad.get('image_preload')
                    button_text = ad.get('button', 'View')
                    click_url = ad.get('link')
                    notification_url = ad.get('notification_url')
                    
                    if not click_url:
                        LOGGER(__name__).warning("RichAds ad missing click URL")
                        return
                    
                    from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup
                    markup = InlineKeyboardMarkup([[
                        InlineKeyboardButton.url(button_text, click_url)
                    ]])
                    
                    # Send ad to user - the image URL loads the tracking pixel
                    ad_sent = False
                    if photo_url:
                        try:
                            await client.send_file(
                                chat_id,
                                photo_url,
                                caption=caption,
                                buttons=markup.to_telethon(),
                                link_preview=False
                            )
                            ad_sent = True
                        except Exception as img_err:
                            LOGGER(__name__).warning(f"Failed to send image ad: {img_err}")
                            # Fallback to text-only ad
                            if caption:
                                await client.send_message(
                                    chat_id,
                                    caption,
                                    buttons=markup.to_telethon(),
                                    link_preview=False
                                )
                                ad_sent = True
                    elif caption:
                        await client.send_message(
                            chat_id,
                            caption,
                            buttons=markup.to_telethon(),
                            link_preview=False
                        )
                        ad_sent = True
                    
                    if not ad_sent:
                        return
                    
                    # CRITICAL: Ping notification_url AFTER ad is sent
                    # This confirms impression to RichAds
                    if notification_url:
                        try:
                            async with session.get(notification_url, timeout=aiohttp.ClientTimeout(total=3)) as notif_resp:
                                if notif_resp.status == 200:
                                    LOGGER(__name__).debug(f"Impression confirmed for user {user_id}")
                                else:
                                    LOGGER(__name__).warning(f"Impression notification returned {notif_resp.status}")
                        except Exception as notif_err:
                            LOGGER(__name__).warning(f"Failed to send impression notification: {notif_err}")
                    
                    # Update cooldown and count
                    self.user_last_ad[user_id] = current_time
                    self.impression_count += 1
                    LOGGER(__name__).info(f"RichAds shown to user {user_id} | Total impressions: {self.impression_count}")
                    
        except aiohttp.ClientTimeout:
            LOGGER(__name__).warning(f"RichAds API timeout for user {user_id}")
        except Exception as e:
            LOGGER(__name__).error(f"Error showing RichAds: {e}")

ad_monetization = AdMonetization()
richads = RichAdsMonetization()
