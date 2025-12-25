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
        LOGGER(__name__).info("RichAds Monetization initialized")
    
    async def show_ad(self, client, chat_id: int, user_id: int, lang_code: str = "en"):
        """Fetch and display RichAds to user with proper implementation per RichAds docs"""
        try:
            user_type = db.get_user_type(user_id)
            
            # Skip ads for premium/admin users
            if user_type in ['paid', 'premium', 'admin']:
                return
            
            # Check cooldown (5 minutes per user)
            current_time = time.time()
            if user_id in self.user_last_ad:
                time_since_last_ad = current_time - self.user_last_ad[user_id]
                if time_since_last_ad < RICHADS_AD_COOLDOWN:
                    return
            
            # Fallback ad to display if API fails
            fallback_caption = (
                "📢 **Check Out Our Latest Offers!**\n\n"
                "Discover amazing deals and exclusive content.\n\n"
                "Click the button below to learn more!"
            )
            
            # Fetch ad from RichAds API - POST request per documentation
            try:
                async with aiohttp.ClientSession() as session:
                    # RichAds requires POST request with JSON body
                    payload = {
                        "language_code": lang_code,
                        "publisher_id": RICHADS_PUBLISHER,
                        "widget_id": RICHADS_WIDGET,
                        "telegram_id": str(user_id),
                        "production": True
                    }
                    
                    async with session.post(RICHADS_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            try:
                                data = await resp.json()
                                
                                # Response is an array of ads
                                if isinstance(data, list) and len(data) > 0:
                                    ad = data[0]
                                    
                                    # Extract fields per RichAds documentation
                                    caption = ad.get('message', '')
                                    image_url = ad.get('image')  # Image with impression tracking embedded
                                    image_preload = ad.get('image_preload')  # Direct image
                                    button_text = ad.get('button', 'View Ad')
                                    click_url = ad.get('link', 'https://example.com')
                                    notification_url = ad.get('notification_url')  # Fire for impression
                                    
                                    from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup
                                    
                                    markup = InlineKeyboardMarkup([[
                                        InlineKeyboardButton.url(button_text, click_url)
                                    ]])
                                    
                                    # Send ad with image if available
                                    if image_url or image_preload:
                                        # Use image_preload if available (direct image), else use image (with tracking)
                                        photo_url = image_preload or image_url
                                        try:
                                            await client.send_file(
                                                chat_id,
                                                photo_url,
                                                caption=caption,
                                                buttons=markup.to_telethon(),
                                                link_preview=False
                                            )
                                        except Exception as e:
                                            LOGGER(__name__).warning(f"Failed to send photo ad: {e}, sending text instead")
                                            # Fallback to text if image fails
                                            await client.send_message(chat_id, caption, buttons=markup.to_telethon(), link_preview=False)
                                    else:
                                        # No image, send text ad
                                        await client.send_message(chat_id, caption, buttons=markup.to_telethon(), link_preview=False)
                                    
                                    # Fire notification URL to track impression per RichAds docs
                                    if notification_url:
                                        try:
                                            async with session.get(notification_url, timeout=aiohttp.ClientTimeout(total=2)):
                                                pass  # Fire and forget
                                        except:
                                            pass
                                    
                                    self.user_last_ad[user_id] = current_time
                                    self.impression_count += 1
                                    LOGGER(__name__).info(f"RichAds Ad shown to user {user_id} | Impressions: {self.impression_count} | Title: {ad.get('title', 'Unknown')} | Brand: {ad.get('brand', 'Unknown')}")
                                    return
                            except Exception as json_err:
                                LOGGER(__name__).warning(f"Could not parse RichAds API response: {json_err}, using fallback ad")
                        else:
                            LOGGER(__name__).warning(f"RichAds API error: {resp.status}")
            
            except Exception as api_err:
                LOGGER(__name__).warning(f"RichAds API request failed: {api_err}, using fallback ad")
            
            # Send fallback ad
            from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup
            
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton.url("View Offers", "https://www.effectivegatecpm.com/zn01rc1vt?key=78d0724d73f6154a582464c95c28210d")
            ]])
            
            await client.send_message(chat_id, fallback_caption, buttons=markup.to_telethon(), link_preview=False)
            self.user_last_ad[user_id] = current_time
            self.impression_count += 1
            LOGGER(__name__).info(f"Fallback ad shown to user {user_id} | Total impressions: {self.impression_count}")
        
        except Exception as e:
            LOGGER(__name__).error(f"Error showing ad (fallback also failed): {e}")


ad_monetization = AdMonetization()
richads = RichAdsMonetization()
