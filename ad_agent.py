import json
import random
import time
import logging
from datetime import datetime, timedelta

# --- Setup Logging ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AdAgent")


class AdAgent:
    """
    The Ad Agent is responsible for selecting optimal ad blocks and scheduling
    them based on various factors, including simulated regional revenue data.
    This demonstrates subgoal decomposition by focusing on ad-specific logic.
    """

    def __init__(self, ad_blocks_data, regional_revenue_data):
        """
        Initializes the Ad Agent with available ad blocks and regional revenue data.
        :param ad_blocks_data: A list of dictionaries, each representing an ad block.
                               Example: [{"id": "ad1", "duration": 30, "target_audience": "kids", "cpm": {"US": 5.0, "EU": 4.5}}]
        :param regional_revenue_data: A dictionary mapping regions to their current
                                      simulated revenue performance (e.g., average CPM).
                                      Example: {"US": {"cpm": 4.8}, "EU": {"cpm": 4.2}}
        """
        self.ad_blocks = ad_blocks_data
        self.regional_revenue = regional_revenue_data
        logger.info("Ad Agent initialized with ad blocks and regional revenue data.")
        logger.debug(f"Ad Blocks: {json.dumps(self.ad_blocks, indent=2)}")
        logger.debug(f"Regional Revenue Data: {json.dumps(self.regional_revenue, indent=2)}")

    @staticmethod  # Changed to staticmethod
    def _get_ad_blocks():
        """
        Provides sample ad block data. In a real system, this would come from a database.
        """
        return [
            {"id": "ad_block_A", "duration": 30, "target_audience": "general",
             "cpm": {"US": 5.0, "EU": 4.5, "ASIA": 3.8}},
            {"id": "ad_block_B", "duration": 60, "target_audience": "kids", "cpm": {"US": 4.0, "EU": 3.5, "ASIA": 3.0}},
            {"id": "ad_block_C", "duration": 30, "target_audience": "sports",
             "cpm": {"US": 6.0, "EU": 5.5, "ASIA": 4.0}},
            {"id": "ad_block_D", "duration": 15, "target_audience": "general",
             "cpm": {"US": 4.8, "EU": 4.0, "ASIA": 3.5}},
            {"id": "ad_block_E", "duration": 60, "target_audience": "luxury",
             "cpm": {"US": 7.5, "EU": 7.0, "ASIA": 6.5}},
        ]

    @staticmethod  # Changed to staticmethod
    def _simulate_regional_revenue_data():
        """
        Simulates dynamic regional revenue data (e.g., average CPM for a region).
        In a real system, this would be fetched from a real-time analytics system.
        """
        regions = ["US", "EU", "ASIA"]
        revenue_data = {}
        for region in regions:
            # Simulate slight fluctuations in CPM
            revenue_data[region] = {"cpm": round(random.uniform(3.0, 7.0), 2)}
        return revenue_data

    def select_ad_block(self, region, target_duration, target_audience="general"):
        """
        Selects the most optimal ad block for a given region and target duration,
        prioritizing higher CPM and matching target audience.
        This is a key subgoal.

        :param region: The target geographical region (e.g., "US", "EU", "ASIA").
        :param target_duration: The desired duration for the ad block in seconds.
        :param target_audience: The target audience for the content (e.g., "kids", "general").
        :return: The selected ad block dictionary or None if no suitable block is found.
        """
        logger.debug(
            f"Thought Chain: Selecting ad block for region: {region}, target duration: {target_duration}s, audience: {target_audience}")

        best_ad = None
        max_cpm = -1

        # Get the current CPM for the region
        region_cpm_factor = self.regional_revenue.get(region, {}).get("cpm", 1.0)  # Default to 1 if no specific data

        for ad in self.ad_blocks:
            ad_cpm_for_region = ad["cpm"].get(region, 0)  # Get specific CPM for the region

            # Apply regional revenue factor (higher factor means higher effective CPM)
            effective_cpm = ad_cpm_for_region * region_cpm_factor

            # Check if duration is close enough (e.g., within 15 seconds)
            duration_match = abs(ad["duration"] - target_duration) <= 15

            # Check audience match (exact match or general ad)
            # A "general" ad matches any target audience.
            # A specific ad (e.g., "kids", "sports") only matches its specific target audience.
            audience_match = (ad["target_audience"] == "general") or (ad["target_audience"] == target_audience)

            if duration_match and audience_match and effective_cpm > max_cpm:
                max_cpm = effective_cpm
                best_ad = ad
                logger.debug(
                    f"Thought Chain: Found better ad candidate: {ad['id']} with effective CPM {effective_cpm:.2f}")

        if best_ad:
            logger.info(f"Selected ad block '{best_ad['id']}' for region '{region}' with effective CPM {max_cpm:.2f}")
        else:
            logger.warning(
                f"No suitable ad block found for region: {region}, duration: {target_duration}s, audience: {target_audience}")

        logger.debug(f"Thought Chain: Ad block selection complete.")
        return best_ad

    def schedule_ads(self, content_schedule):
        """
        Simulates scheduling ad blocks into content slots.
        This demonstrates the higher-level task using the ad block selection subgoal.

        :param content_schedule: A list of content slots, each a dictionary with:
                                 {"content_id": "ep1", "region": "US", "duration": 1200, "ad_slot_duration": 60, "target_audience": "general"}
        :return: A list of scheduled ad slots.
        """
        logger.info("Starting ad scheduling process.")
        scheduled_ads = []
        for slot in content_schedule:
            logger.debug(f"Thought Chain: Processing content slot for {slot['content_id']} in {slot['region']}")
            ad_block = self.select_ad_block(
                region=slot["region"],
                target_duration=slot["ad_slot_duration"],
                target_audience=slot.get("target_audience", "general")
            )

            if ad_block:
                scheduled_ads.append({
                    "content_id": slot["content_id"],
                    "region": slot["region"],
                    "scheduled_ad_id": ad_block["id"],
                    "scheduled_ad_duration": ad_block["duration"],
                    "scheduled_at": datetime.now().isoformat(),
                    "effective_cpm": ad_block["cpm"].get(slot["region"], 0) * self.regional_revenue.get(slot["region"],
                                                                                                        {}).get("cpm",
                                                                                                                1.0)
                })
                logger.info(
                    f"Scheduled ad '{ad_block['id']}' for content '{slot['content_id']}' in region '{slot['region']}'.")
            else:
                scheduled_ads.append({
                    "content_id": slot["content_id"],
                    "region": slot["region"],
                    "scheduled_ad_id": "NO_AD_FOUND",
                    "scheduled_ad_duration": 0,
                    "scheduled_at": datetime.now().isoformat(),
                    "effective_cpm": 0
                })
                logger.warning(
                    f"Could not schedule an ad for content '{slot['content_id']}' in region '{slot['region']}'.")
            logger.debug(f"Thought Chain: Finished processing content slot for {slot['content_id']}.")

        logger.info("Ad scheduling process completed.")
        return scheduled_ads


# --- Main execution block for demonstration and verification ---
if __name__ == "__main__":
    logger.info("--- Starting Ad Agent Subgoal Decomposition Test ---")

    # 1. Simulate initial regional revenue data
    initial_regional_revenue = {
        "US": {"cpm": 5.0},
        "EU": {"cpm": 4.0},
        "ASIA": {"cpm": 3.5}
    }

    # 2. Define sample ad blocks
    sample_ad_blocks = [
        {"id": "ad_block_A", "duration": 30, "target_audience": "general", "cpm": {"US": 5.0, "EU": 4.5, "ASIA": 3.8}},
        {"id": "ad_block_B", "duration": 60, "target_audience": "kids", "cpm": {"US": 4.0, "EU": 3.5, "ASIA": 3.0}},
        {"id": "ad_block_C", "duration": 30, "target_audience": "sports", "cpm": {"US": 6.0, "EU": 5.5, "ASIA": 4.0}},
        {"id": "ad_block_D", "duration": 15, "target_audience": "general", "cpm": {"US": 4.8, "EU": 4.0, "ASIA": 3.5}},
        {"id": "ad_block_E", "duration": 60, "target_audience": "luxury", "cpm": {"US": 7.5, "EU": 7.0, "ASIA": 6.5}},
        {"id": "ad_block_F", "duration": 30, "target_audience": "kids", "cpm": {"US": 4.2, "EU": 3.7, "ASIA": 3.2}},
        # Another kids ad
    ]

    ad_agent = AdAgent(sample_ad_blocks, initial_regional_revenue)

    # 3. Define content slots that need ads
    content_slots_to_schedule = [
        {"content_id": "episode_101", "region": "US", "ad_slot_duration": 30, "target_audience": "general"},
        {"content_id": "kids_show_ep_05", "region": "EU", "ad_slot_duration": 60, "target_audience": "kids"},
        {"content_id": "sports_highlight_reel", "region": "US", "ad_slot_duration": 30, "target_audience": "sports"},
        {"content_id": "travel_doc_asia", "region": "ASIA", "ad_slot_duration": 45, "target_audience": "general"},
        # No exact 45s ad
        {"content_id": "luxury_lifestyle_vlog", "region": "EU", "ad_slot_duration": 60, "target_audience": "luxury"},
    ]

    # 4. Simulate ad scheduling
    scheduled_ads = ad_agent.schedule_ads(content_slots_to_schedule)

    logger.info("\n--- Simulated Ad Schedule Output ---")
    for ad_entry in scheduled_ads:
        logger.info(json.dumps(ad_entry, indent=2))

    # --- Verification of Output ---
    logger.info("\n--- Verifying Ad Scheduling Decisions ---")

    # Verification 1: Check specific ad selection for a known optimal case
    # For episode_101 (US, 30s, general), ad_block_A (US CPM 5.0) is the correct choice because ad_block_C is for 'sports' audience.
    expected_ad_ep1 = "ad_block_A"  # Corrected expectation
    actual_ad_ep1 = next((ad["scheduled_ad_id"] for ad in scheduled_ads if ad["content_id"] == "episode_101"), None)
    if actual_ad_ep1 == expected_ad_ep1:
        logger.info(f"Verification PASSED: For 'episode_101', expected '{expected_ad_ep1}', got '{actual_ad_ep1}'.")
    else:
        logger.error(f"Verification FAILED: For 'episode_101', expected '{expected_ad_ep1}', got '{actual_ad_ep1}'.")

    # Verification 2: Check ad selection for kids content
    # For kids_show_ep_05 (EU, 60s, kids), ad_block_B (EU CPM 3.5) is the correct choice because ad_block_F (30s) is too short.
    expected_ad_kids = "ad_block_B"  # Corrected expectation
    actual_ad_kids = next((ad["scheduled_ad_id"] for ad in scheduled_ads if ad["content_id"] == "kids_show_ep_05"),
                          None)
    if actual_ad_kids == expected_ad_kids:
        logger.info(
            f"Verification PASSED: For 'kids_show_ep_05', expected '{expected_ad_kids}', got '{actual_ad_kids}'.")
    else:
        logger.error(
            f"Verification FAILED: For 'kids_show_ep_05', expected '{expected_ad_kids}', got '{actual_ad_kids}'.")

    # Verification 3: Check handling of no exact duration match (should pick closest within tolerance)
    # For travel_doc_asia (ASIA, 45s, general), ad_block_A (30s) or D (15s) are closest.
    # Ad_block_A (ASIA CPM 3.8) vs Ad_block_D (ASIA CPM 3.5). A is better.
    expected_ad_travel = "ad_block_A"
    actual_ad_travel = next((ad["scheduled_ad_id"] for ad in scheduled_ads if ad["content_id"] == "travel_doc_asia"),
                            None)
    if actual_ad_travel == expected_ad_travel:
        logger.info(
            f"Verification PASSED: For 'travel_doc_asia', expected '{expected_ad_travel}', got '{actual_ad_travel}'.")
    else:
        logger.error(
            f"Verification FAILED: For 'travel_doc_asia', expected '{expected_ad_travel}', got '{actual_ad_travel}'.")

    # Verification 4: Check if effective CPM is calculated and present
    for ad_entry in scheduled_ads:
        if ad_entry["scheduled_ad_id"] != "NO_AD_FOUND" and "effective_cpm" in ad_entry:
            logger.info(
                f"Verification PASSED: Effective CPM present for {ad_entry['scheduled_ad_id']} in {ad_entry['region']}.")
        elif ad_entry["scheduled_ad_id"] != "NO_AD_FOUND":
            logger.error(
                f"Verification FAILED: Effective CPM missing for {ad_entry['scheduled_ad_id']} in {ad_entry['region']}.")

    logger.info("--- Ad Agent Subgoal Decomposition Test Completed ---")
