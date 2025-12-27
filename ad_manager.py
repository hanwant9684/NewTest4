# Copyright (C) @Wolfy004
# Channel: https://t.me/Wolfy004
# Ad Manager - Handles fallback between multiple ad networks

from logger import LOGGER
from richads import richads
from adsgram import adsgram

class AdManager:
    """
    Manages ad display with fallback strategy.
    Tries RichAds first, then falls back to AdsGram if RichAds fails.
    """
    
    async def send_ad_with_fallback(self, bot, user_id: int, chat_id: int, language_code: str = "en", is_premium: bool = False, is_admin: bool = False) -> bool:
        """
        Try to send an ad using RichAds, fallback to AdsGram if it fails.
        Skips ads for premium and admin users.
        
        Args:
            bot: Telegram bot client
            user_id: Telegram user ID
            chat_id: Chat ID where to send the ad
            language_code: Language code for the ad
            is_premium: Whether user is premium
            is_admin: Whether user is admin
        
        Returns:
            True if ad was sent, False otherwise
        """
        # Skip ads for premium and admin users
        if is_premium or is_admin:
            LOGGER(__name__).debug(f"Skipped ads for user {user_id} (premium: {is_premium}, admin: {is_admin})")
            return False
        
        # Try RichAds first
        if richads.is_enabled():
            try:
                success = await richads.send_ad_to_user(bot, chat_id, language_code)
                if success:
                    LOGGER(__name__).info(f"Ad: RichAds shown | User: {user_id}")
                    return True
            except Exception as e:
                LOGGER(__name__).debug(f"RichAds failed: {str(e)[:50]}")
        
        # Fallback to AdsGram
        if adsgram.is_enabled():
            try:
                success = await adsgram.send_ad_to_user(bot, chat_id, user_id, language_code)
                if success:
                    LOGGER(__name__).info(f"Ad: AdsGram shown | User: {user_id}")
                    return True
            except Exception as e:
                LOGGER(__name__).debug(f"AdsGram failed: {str(e)[:50]}")
        
        LOGGER(__name__).debug(f"Ad: No ads available | User: {user_id}")
        return False
    
    def is_any_enabled(self) -> bool:
        """Check if any ad network is enabled"""
        return richads.is_enabled() or adsgram.is_enabled()


# Global instance
ad_manager = AdManager()
