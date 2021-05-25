from tap_facebook_pages.streams import PageInsights, PostInsights

# TODO The insights tables can be further split if the insights with _unique postfix get their dedicated tables

INSIGHT_STREAMS = [

    # PAGE INSIGHTS

    # page_insight_CTA_clicks
    {
        "class": PageInsights,
        "name": "page_insight_CTA_clicks",
        "metrics": [
            "page_total_actions",
            "page_cta_clicks_logged_in_total",
            "page_cta_clicks_logged_in_unique",
            "page_cta_clicks_by_site_logged_in_unique",
            "page_cta_clicks_by_age_gender_logged_in_unique",
            "page_cta_clicks_logged_in_by_country_unique",
            "page_cta_clicks_logged_in_by_city_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_call_phone_clicks",
        "metrics": [
            "page_call_phone_clicks_logged_in_unique",
            "page_call_phone_clicks_by_age_gender_logged_in_unique",
            "page_call_phone_clicks_logged_in_by_country_unique",
            "page_call_phone_clicks_logged_in_by_city_unique",
            "page_call_phone_clicks_by_site_logged_in_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_get_directions_clicks",
        "metrics": [
            "page_get_directions_clicks_logged_in_unique",
            "page_get_directions_clicks_by_age_gender_logged_in_unique",
            "page_get_directions_clicks_logged_in_by_country_unique",
            "page_get_directions_clicks_logged_in_by_city_unique",
            "page_get_directions_clicks_by_site_logged_in_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_website_clicks",
        "metrics": [
            "page_website_clicks_logged_in_unique",
            "page_website_clicks_by_age_gender_logged_in_unique",
            "page_website_clicks_logged_in_by_country_unique",
            "page_website_clicks_logged_in_by_city_unique",
            "page_website_clicks_by_site_logged_in_unique",
        ]
    },
    # page_insight_engagement
    {
        "class": PageInsights,
        "name": "page_insight_engagement",
        "metrics": [
            "page_engaged_users",
            "page_post_engagements",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_consumptions",
        "metrics": [
            "page_consumptions",
            "page_consumptions_unique",
            "page_consumptions_by_consumption_type",
            "page_consumptions_by_consumption_type_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_places_checkin",
        "metrics": [
            "page_places_checkin_total",
            "page_places_checkin_total_unique",
            "page_places_checkin_mobile",
            "page_places_checkin_mobile_unique",
            "page_places_checkins_by_age_gender",
            "page_places_checkins_by_locale",
            "page_places_checkins_by_country",

        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_feedback",
        "metrics": [
            "page_negative_feedback",
            "page_negative_feedback_unique",
            "page_negative_feedback_by_type",
            "page_negative_feedback_by_type_unique",
            "page_positive_feedback_by_type",
            "page_positive_feedback_by_type_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_fans",
        "metrics": [
            "page_fans_online",
            "page_fans_online_per_day",
            "page_fan_adds_by_paid_non_paid_unique",
        ]
    },
    # page_insight_impressions
    {
        "class": PageInsights,
        "name": "page_insight_impressions",
        "metrics": [
            "page_impressions",
            "page_impressions_unique",
            "page_impressions_paid",
            "page_impressions_paid_unique",
            "page_impressions_organic",
            "page_impressions_organic_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_impressions_2",
        "metrics": [
            "page_impressions_viral",
            "page_impressions_viral_unique",
            "page_impressions_nonviral",
            "page_impressions_nonviral_unique",
            "page_impressions_frequency_distribution",
            "page_impressions_viral_frequency_distribution",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_impressions_by_category",
        "metrics": [
            "page_impressions_by_story_type",
            "page_impressions_by_story_type_unique",
            "page_impressions_by_age_gender_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_impressions_by_location",
        "metrics": [
            "page_impressions_by_city_unique",
            "page_impressions_by_country_unique",
            "page_impressions_by_locale_unique",
        ]
    },
    # page_insight_post
    {
        "class": PageInsights,
        "name": "page_insight_post",
        "metrics": [
            "page_posts_impressions",
            "page_posts_impressions_unique",
            "page_posts_impressions_paid",
            "page_posts_impressions_paid_unique",
            "page_posts_impressions_organic",
            "page_posts_impressions_organic_unique",
            "page_posts_served_impressions_organic_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_post_2",
        "metrics": [
            "page_posts_impressions_viral",
            "page_posts_impressions_viral_unique",
            "page_posts_impressions_nonviral",
            "page_posts_impressions_nonviral_unique",
            "page_posts_impressions_frequency_distribution",
        ]
    },
    # page_insight_reactions
    {
        "class": PageInsights,
        "name": "page_insight_reactions",
        "metrics": [
            "page_actions_post_reactions_like_total",
            "page_actions_post_reactions_love_total",
            "page_actions_post_reactions_wow_total",
            "page_actions_post_reactions_haha_total",
            "page_actions_post_reactions_sorry_total",
            "page_actions_post_reactions_anger_total",
            "page_actions_post_reactions_total",
        ]
    },
    # page_insight_demographics
    {
        "class": PageInsights,
        "name": "page_insight_demographics",
        "metrics": [
            "page_fans",
            "page_fans_locale",
            "page_fans_city",
            "page_fans_country",
            "page_fans_gender_age",
            "page_fans_by_unlike_source_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_demographics_2",
        "metrics": [
            "page_fan_adds",
            "page_fan_adds_unique",
            "page_fans_by_like_source",
            "page_fans_by_like_source_unique",
            "page_fan_removes",
            "page_fan_removes_unique",
        ]
    },
    # page_insight_video_views
    {
        "class": PageInsights,
        "name": "page_insight_video_views",
        "metrics": [
            "page_video_views",
            "page_video_views_paid",
            "page_video_views_organic",
            "page_video_views_by_paid_non_paid",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_video_views_2",
        "metrics": [
            "page_video_views_autoplayed",
            "page_video_views_click_to_play",
            "page_video_views_unique",
            "page_video_repeat_views",
            "page_video_view_time",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_video_complete_views",
        "metrics": [
            "page_video_complete_views_30s",
            "page_video_complete_views_30s_paid",
            "page_video_complete_views_30s_organic",
            "page_video_complete_views_30s_autoplayed",
            "page_video_complete_views_30s_click_to_play",
            "page_video_complete_views_30s_unique",
            "page_video_complete_views_30s_repeat_views",
            "post_video_complete_views_30s_autoplayed",
            "post_video_complete_views_30s_clicked_to_play",
            "post_video_complete_views_30s_organic",
            "post_video_complete_views_30s_paid",
            "post_video_complete_views_30s_unique",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_video_10s_views",
        "metrics": [
            "page_video_views_10s",
            "page_video_views_10s_paid",
            "page_video_views_10s_organic",
            "page_video_views_10s_autoplayed",
            "page_video_views_10s_click_to_play",
            "page_video_views_10s_unique",
            "page_video_views_10s_repeat",
        ]
    },
    # page_insight_views
    {
        "class": PageInsights,
        "name": "page_insight_views",
        "metrics": [
            "page_views_total",
            "page_views_logout",
            "page_views_logged_in_total",
            "page_views_logged_in_unique",
            "page_views_external_referrals",
        ]
    },
    {
        "class": PageInsights,
        "name": "page_insight_views_by_category",
        "metrics": [
            "page_views_by_profile_tab_total",
            "page_views_by_profile_tab_logged_in_unique",
            "page_views_by_internal_referer_logged_in_unique",
            "page_views_by_site_logged_in_unique",
            "page_views_by_age_gender_logged_in_unique",
            "page_views_by_referers_logged_in_unique",
        ]
    },
    # page_insight_video_ad_break
    {
        "class": PageInsights,
        "name": "page_insight_video_ad_break",
        "metrics": [
            "page_daily_video_ad_break_ad_impressions_by_crosspost_status",
            "page_daily_video_ad_break_cpm_by_crosspost_status",
            "page_daily_video_ad_break_earnings_by_crosspost_status",
        ]
    },

    # POST INSIGHTS
    # post_insight_engagement
    {
        "class": PostInsights,
        "name": "post_insight_engagement",
        "metrics": [
            "post_engaged_users",
            "post_negative_feedback",
            "post_negative_feedback_unique",
            "post_negative_feedback_by_type",
            "post_negative_feedback_by_type_unique",
            "post_engaged_fan",
            "post_clicks",
            "post_clicks_unique",
            "post_clicks_by_type",
            "post_clicks_by_type_unique",
        ]
    },
    # post_insight_impressions
    {
        "class": PostInsights,
        "name": "post_insight_impressions",
        "metrics": [
            "post_impressions",
            "post_impressions_unique",
            "post_impressions_paid",
            "post_impressions_paid_unique",
            "post_impressions_fan",
            "post_impressions_fan_unique",
            "post_impressions_fan_paid",
            "post_impressions_fan_paid_unique",
            "post_impressions_organic",
            "post_impressions_organic_unique",
            "post_impressions_viral",
            "post_impressions_viral_unique",
            "post_impressions_nonviral",
            "post_impressions_nonviral_unique",
            "post_impressions_by_story_type",
            "post_impressions_by_story_type_unique",
        ]
    },
    # post_insight_reactions
    {
        "class": PostInsights,
        "name": "post_insight_reactions",
        "metrics": [
            "post_reactions_like_total",
            "post_reactions_love_total",
            "post_reactions_wow_total",
            "post_reactions_haha_total",
            "post_reactions_sorry_total",
            "post_reactions_anger_total",
            "post_reactions_by_type_total",
        ]
    },
    # post_insight_video
    {
        "class": PostInsights,
        "name": "post_insight_video",
        "metrics": [
            "post_video_avg_time_watched",
            "post_video_retention_graph",
            "post_video_retention_graph_clicked_to_play",
            "post_video_retention_graph_autoplayed",
            "post_video_length",
        ]
    },
    {
        "class": PostInsights,
        "name": "post_insight_video_views",
        "metrics": [
            "post_video_views_organic",
            "post_video_views_organic_unique",
            "post_video_views_paid",
            "post_video_views_paid_unique",
            "post_video_views",
            "post_video_views_unique",
            "post_video_views_autoplayed",
            "post_video_views_clicked_to_play",
            "post_video_views_sound_on",
            "post_video_view_time",
            "post_video_view_time_organic",
        ]
    },
    {
        "class": PostInsights,
        "name": "post_insight_video_complete_views",
        "metrics": [
            "post_video_complete_views_organic",
            "post_video_complete_views_organic_unique",
            "post_video_complete_views_paid",
            "post_video_complete_views_paid_unique",
        ]
    },
    {
        "class": PostInsights,
        "name": "post_insight_video_by_sec",
        "metrics": [
            "post_video_views_15s",
            "post_video_views_60s_excludes_shorter",
            "post_video_views_10s",
            "post_video_views_10s_unique",
            "post_video_views_10s_autoplayed",
            "post_video_views_10s_clicked_to_play",
            "post_video_views_10s_organic",
            "post_video_views_10s_paid",
            "post_video_views_10s_sound_on",

        ]
    },
    {
        "class": PostInsights,
        "name": "post_insight_video_by_category",
        "metrics": [
            "post_video_view_time_by_age_bucket_and_gender",
            "post_video_view_time_by_region_id",
            "post_video_views_by_distribution_type",
            "post_video_view_time_by_distribution_type",
            "post_video_view_time_by_country_id",
        ]
    },
    # post_insight_activity
    {
        "class": PostInsights,
        "name": "post_insight_activity",
        "metrics": [
            "post_activity",
            "post_activity_unique",
            "post_activity_by_action_type",
            "post_activity_by_action_type_unique",
        ]
    },

    # post_insight_video_ad_break
    {
        "class": PostInsights,
        "name": "post_insight_video_ad_break",
        "metrics": [
            "post_video_ad_break_ad_impressions",
            "post_video_ad_break_earnings",
            "post_video_ad_break_ad_cpm",
        ]
    },

]
