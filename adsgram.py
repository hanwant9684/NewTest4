# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004
# AdsGram Telegram Bot Integration

import os
import json
import aiohttp
from typing import Optional, Dict, Any
from logger import LOGGER
from telethon_helpers import InlineKeyboardButton, InlineKeyboardMarkup

ADSGRAM_API_URL = "https://api.adsgram.ai/advbot"

class AdsGramManager:
    def __init__(self):
        self.block_id = os.getenv("ADSGRAM_BLOCK_ID", "")
        
        if self.block_id:
            LOGGER(__name__).info(f"AdsGram initialized - Block ID: {self.block_id}")
        else:
            LOGGER(__name__).warning("AdsGram not configured - ADSGRAM_BLOCK_ID not set")
    
    def is_enabled(self) -> bool:
        """Check if AdsGram is configured"""
        return bool(self.block_id)
    
    async def fetch_ad(self, user_id: int, language_code: str = "en") -> Optional[Dict[str, Any]]:
        """Fetch ad from AdsGram API"""
        if not self.is_enabled():
            return None
        
        # Extract numeric part of block_id if it has prefix
        block_id = self.block_id.replace("bot-", "") if "bot-" in self.block_id else self.block_id
        
        params = {
            "blockid": block_id,
            "tgid": str(user_id),
            "language": language_code[:2].lower() if language_code else "en"
        }
        
        try:
            LOGGER(__name__).info(f"AdsGram: Fetching ad with params: {params}")
            async with aiohttp.ClientSession() as session:
                async with session.get(ADSGRAM_API_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    LOGGER(__name__).info(f"AdsGram API response status: {response.status}")
                    
                    # Check content type before parsing JSON
                    content_type = response.headers.get('Content-Type', '')
                    
                    if response.status == 200:
                        # Get response as text first
                        response_text = await response.text()
                        
                        # Try to parse as JSON
                        try:
                            ad = json.loads(response_text)
                            
                            if ad and ad.get("text_html"):
                                LOGGER(__name__).debug(f"AdsGram: Got ad for user {user_id}")
                                return ad
                            LOGGER(__name__).debug(f"AdsGram: No ads available for user {user_id}")
                            return None
                        except json.JSONDecodeError:
                            # Not valid JSON - block may not be active
                            LOGGER(__name__).debug(f"AdsGram: Invalid response (block may not be active)")
                            return None
                    else:
                        response_text = await response.text()
                        LOGGER(__name__).warning(f"AdsGram API error: {response.status} - {response_text}")
                        return None
        except Exception as e:
            LOGGER(__name__).error(f"AdsGram fetch error: {e}", exc_info=True)
            return None
    
    async def send_ad_to_user(self, client, chat_id: int, user_id: int, language_code: str = "en") -> bool:
        """Fetch and send AdsGram ad to user"""
        if not self.is_enabled():
            return False
        
        ad = await self.fetch_ad(user_id=user_id, language_code=language_code)
        
        if not ad:
            return False
        
        try:
            # Build buttons from AdsGram response (as per AdsGram documentation)
            buttons_list = []
            
            # Add click button (main ad link)
            if ad.get("click_url") and ad.get("button_name"):
                buttons_list.append(InlineKeyboardButton.url(
                    ad['button_name'],  # AdsGram provides button text
                    ad["click_url"]
                ))
            
            # Add reward button (only active after clicking main ad)
            if ad.get("reward_url"):
                # Use custom text instead of AdsGram's button_reward_name
                reward_text = "5 Free Downloads"
                buttons_list.append(InlineKeyboardButton.url(
                    reward_text,
                    ad["reward_url"]
                ))
            
            # Create keyboard with both buttons
            markup = InlineKeyboardMarkup([[button] for button in buttons_list]) if buttons_list else None
            
            # Send ad with image or text
            if ad.get("image_url"):
                await client.send_file(
                    chat_id,
                    file=ad["image_url"],
                    caption=ad.get("text_html", ""),
                    buttons=markup.to_telethon() if markup else None,
                    parse_mode='html'
                )
            else:
                # Fallback to text if no image
                await client.send_message(
                    chat_id,
                    ad.get("text_html", "Ad"),
                    buttons=markup.to_telethon() if markup else None,
                    parse_mode='html'
                )
            
            return True
            
        except Exception as e:
            LOGGER(__name__).warning(f"AdsGram send error: {str(e)[:100]}")
            return False


# Global instance
adsgram = AdsGramManager()
