# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004
# RichAds Telegram Bot Integration

import os
import html
import aiohttp
from typing import Optional, Dict, Any, List
from logger import LOGGER
from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup

RICHADS_API_URL = "http://15068.xml.adx1.com/telegram-mb"

class RichAdsManager:
    def __init__(self):
        self.publisher_id = os.getenv("RICHADS_PUBLISHER_ID", "")
        self.widget_id = os.getenv("RICHADS_WIDGET_ID", "")
        self.production = os.getenv("RICHADS_PRODUCTION", "true").lower() == "true"
        
        if self.publisher_id:
            LOGGER(__name__).info(f"RichAds initialized - Publisher: {self.publisher_id}, Production: {self.production}")
        else:
            LOGGER(__name__).warning("RichAds not configured - RICHADS_PUBLISHER_ID not set")
    
    def is_enabled(self) -> bool:
        """Check if RichAds is configured"""
        return bool(self.publisher_id)
    
    async def fetch_ad(self, language_code: str = "en", telegram_id: str = None) -> Optional[List[Dict[str, Any]]]:
        """Fetch ad from RichAds API"""
        if not self.is_enabled():
            return None
            
        payload = {
            "language_code": language_code[:2].lower() if language_code else "en",
            "publisher_id": self.publisher_id,
            "production": self.production
        }
        
        if self.widget_id:
            payload["widget_id"] = self.widget_id
        if telegram_id:
            payload["telegram_id"] = str(telegram_id)
        
        try:
            LOGGER(__name__).info(f"RichAds: Fetching ads with payload: {payload}")
            async with aiohttp.ClientSession() as session:
                async with session.post(RICHADS_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    LOGGER(__name__).info(f"RichAds API response status: {response.status}")
                    if response.status == 200:
                        ads = await response.json()
                        LOGGER(__name__).info(f"RichAds API response: {ads}")
                        if ads and len(ads) > 0:
                            LOGGER(__name__).info(f"RichAds: Received {len(ads)} ad(s) for user {telegram_id}")
                            return ads
                        LOGGER(__name__).debug(f"RichAds: No ads available for user {telegram_id}")
                        return None
                    else:
                        response_text = await response.text()
                        LOGGER(__name__).warning(f"RichAds API error: {response.status} - {response_text}")
                        return None
        except Exception as e:
            LOGGER(__name__).error(f"RichAds fetch error: {e}", exc_info=True)
            return None
    
    async def notify_impression(self, notification_url: str) -> bool:
        """Notify RichAds that ad impression happened"""
        try:
            LOGGER(__name__).info(f"RichAds: Notifying impression with URL: {notification_url}")
            async with aiohttp.ClientSession() as session:
                async with session.get(notification_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    LOGGER(__name__).info(f"RichAds impression notification response: {response.status}")
                    if response.status == 200:
                        LOGGER(__name__).info("RichAds impression notified successfully")
                        return True
                    LOGGER(__name__).warning(f"RichAds impression failed with status: {response.status}")
                    return False
        except Exception as e:
            LOGGER(__name__).error(f"RichAds impression notify error: {e}", exc_info=True)
            return False
    
    async def send_ad_to_user(self, client, chat_id: int, language_code: str = "en") -> bool:
        """Fetch and send RichAd to user as photo message"""
        if not self.is_enabled():
            LOGGER(__name__).debug("RichAds not enabled")
            return False
            
        ads = await self.fetch_ad(language_code=language_code, telegram_id=str(chat_id))
        
        if not ads or len(ads) == 0:
            LOGGER(__name__).debug(f"RichAds: No ads returned for user {chat_id}")
            return False
        
        ad = ads[0]  # Use first ad
        
        try:
            # Decode HTML entities in URLs (RichAds returns &amp; instead of &)
            notification_url = ad.get("notification_url", "")
            if notification_url:
                notification_url = html.unescape(notification_url)
                ad["notification_url"] = notification_url
            
            click_url = ad.get("link", "")
            if click_url:
                click_url = html.unescape(click_url)
            
            image_url = ad.get("image") or ad.get("image_preload")
            if image_url:
                image_url = html.unescape(image_url)
            
            # Build caption from title and message
            caption = ""
            if ad.get("title"):
                caption += f"**{ad['title']}**\n"
            if ad.get("message"):
                caption += ad["message"]
            if ad.get("brand"):
                caption += f"\n\n🏷️ {ad['brand']}"
            
            # Add sponsored label
            caption = "📢 **Sponsored**\n\n" + caption
            
            # Build inline button
            button_text = ad.get("button", "Learn More")
            
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton.url(f"👉 {button_text}", click_url)]
            ])
            
            if image_url:
                await client.send_file(
                    chat_id,
                    file=image_url,
                    caption=caption,
                    buttons=buttons.to_telethon(),
                    parse_mode='md'
                )
            else:
                # Fallback to text message if no image
                await client.send_message(
                    chat_id,
                    caption,
                    buttons=buttons.to_telethon(),
                    parse_mode='md'
                )
            
            # Notify impression
            if ad.get("notification_url"):
                await self.notify_impression(ad["notification_url"])
            
            LOGGER(__name__).info(f"RichAd sent to user {chat_id}")
            return True
            
        except Exception as e:
            LOGGER(__name__).error(f"Error sending RichAd to {chat_id}: {e}")
            return False


# Global instance
richads = RichAdsManager()
